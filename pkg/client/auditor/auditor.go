/*
Copyright 2021 CodeNotary, Inc. All rights reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

	http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package auditor

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"regexp"
	"strings"
	"time"

	"github.com/codenotary/immudb/embedded/store"
	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/codenotary/immudb/pkg/auth"
	"github.com/codenotary/immudb/pkg/client"
	"github.com/codenotary/immudb/pkg/client/cache"
	"github.com/codenotary/immudb/pkg/client/state"
	"github.com/codenotary/immudb/pkg/client/timestamp"
	"github.com/codenotary/immudb/pkg/logger"
	"github.com/golang/protobuf/ptypes/empty"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/types/known/emptypb"
)

// Auditor the auditor interface
type Auditor interface {
	Run(interval time.Duration, singleRun bool, stopc <-chan struct{}, donec chan<- struct{}) error
}

// AuditNotificationConfig holds the URL and credentials used to publish audit
// result to ledger compliance.
type AuditNotificationConfig struct {
	URL            string
	Username       string
	Password       string
	RequestTimeout time.Duration

	publishFunc func(*http.Request) (*http.Response, error)
}

type defaultAuditor struct {
	index               uint64
	databaseIndex       int
	logger              logger.Logger
	serverAddress       string
	dialOptions         []grpc.DialOption
	history             cache.HistoryCache
	ts                  client.TimestampService
	username            []byte
	databases           []string
	password            []byte
	auditDatabases      []string
	serverSigningPubKey *ecdsa.PublicKey
	notificationConfig  AuditNotificationConfig
	serviceClient       schema.ImmuServiceClient
	uuidProvider        state.UUIDProvider

	slugifyRegExp *regexp.Regexp
	updateMetrics func(string, string, bool, bool, bool, *schema.ImmutableState, *schema.ImmutableState)

	monitoringHTTPPort *int
}

// DefaultAuditor creates initializes a default auditor implementation
func DefaultAuditor(
	interval time.Duration,
	serverAddress string,
	dialOptions *[]grpc.DialOption,
	username string,
	passwordBase64 string,
	auditDatabases []string,
	serverSigningPubKey *ecdsa.PublicKey,
	notificationConfig AuditNotificationConfig,
	serviceClient schema.ImmuServiceClient,
	uuidProvider state.UUIDProvider,
	history cache.HistoryCache,
	updateMetrics func(string, string, bool, bool, bool, *schema.ImmutableState, *schema.ImmutableState),
	log logger.Logger,
	monitoringHTTPPort *int) (Auditor, error) {

	password, err := auth.DecodeBase64Password(passwordBase64)
	if err != nil {
		return nil, err
	}

	dt, _ := timestamp.NewDefaultTimestamp()

	slugifyRegExp, _ := regexp.Compile(`[^a-zA-Z0-9\-_]+`)

	httpClient := &http.Client{Timeout: notificationConfig.RequestTimeout}
	notificationConfig.publishFunc = httpClient.Do

	return &defaultAuditor{
		0,
		0,
		log,
		serverAddress,
		*dialOptions,
		history,
		client.NewTimestampService(dt),
		[]byte(username),
		nil,
		[]byte(password),
		auditDatabases,
		serverSigningPubKey,
		notificationConfig,
		serviceClient,
		uuidProvider,
		slugifyRegExp,
		updateMetrics,
		monitoringHTTPPort,
	}, nil
}

func (a *defaultAuditor) Run(
	interval time.Duration,
	singleRun bool,
	stopc <-chan struct{},
	donec chan<- struct{},
) (err error) {
	defer func() { donec <- struct{}{} }()
	a.logger.Infof("starting auditor with a %s interval ...", interval)

	if singleRun {
		err = a.audit()
	} else {
		// start monitoring HTTP server
		var monitoringServer *http.Server
		if a.monitoringHTTPPort != nil {
			a.logger.Infof("auditor monitoring HTTP server starting on port %d ...", *a.monitoringHTTPPort)
			go func() {
				monitoringServer = StartHTTPServerForMonitoring(
					fmt.Sprintf("0.0.0.0:%d", *a.monitoringHTTPPort),
					func(httpServer *http.Server) error { return httpServer.ListenAndServe() },
					a.logger,
					a.serviceClient)
			}()
		}
		defer func() {
			if monitoringServer != nil {
				a.logger.Debugf("auditor monitoring HTTP server stopped")
				monitoringServer.Close()
			}
		}()

		err = repeat(interval, stopc, a.audit)
		if err != nil {
			return err
		}
	}
	a.logger.Infof("auditor stopped")
	return err
}

func (a *defaultAuditor) audit() error {
	start := time.Now()
	a.index++
	a.logger.Infof("audit #%d started @ %s", a.index, start)

	verified := true
	checked := false
	withError := false
	serverID := "unknown"
	var prevState *schema.ImmutableState
	var state *schema.ImmutableState

	defer func() {
		a.updateMetrics(
			serverID, a.serverAddress, checked, withError, verified, prevState, state)
	}()

	// returning an error would completely stop the auditor process
	var noErr error

	ctx := context.Background()
	loginResponse, err := a.serviceClient.Login(ctx, &schema.LoginRequest{
		User:     a.username,
		Password: a.password,
	})
	if err != nil {
		a.logger.Errorf("error logging in with user %s: %v", a.username, err)
		withError = true
		return noErr
	}
	defer a.serviceClient.Logout(ctx, &empty.Empty{})

	md := metadata.Pairs("authorization", loginResponse.Token)
	ctx = metadata.NewOutgoingContext(context.Background(), md)

	//check if we have cycled through the list of databases
	if a.databaseIndex == len(a.databases) {
		//if we have reached the end get a fresh list of dbs that belong to the user
		dbs, err := a.serviceClient.DatabaseList(ctx, &emptypb.Empty{})
		if err != nil {
			a.logger.Errorf("error getting a list of databases %v", err)
			withError = true
			return noErr
		}
		a.databases = nil

		for _, db := range dbs.Databases {
			dbMustBeAudited := len(a.auditDatabases) <= 0
			for _, dbPrefix := range a.auditDatabases {
				if strings.HasPrefix(db.DatabaseName, dbPrefix) {
					dbMustBeAudited = true
					break
				}
			}
			if dbMustBeAudited {
				a.databases = append(a.databases, db.DatabaseName)
			}
		}

		a.databaseIndex = 0
		if len(a.databases) <= 0 {
			a.logger.Errorf(
				"audit #%d aborted: no databases to audit found after (re)loading the list of databases",
				a.index)
			withError = true
			return noErr
		}

		a.logger.Infof(
			"audit #%d - list of databases to audit has been (re)loaded - %d database(s) found: %v",
			a.index, len(a.databases), a.databases)
	}

	dbName := a.databases[a.databaseIndex]
	resp, err := a.serviceClient.UseDatabase(ctx, &schema.Database{
		DatabaseName: dbName,
	})
	if err != nil {
		a.logger.Errorf("error selecting database %s: %v", dbName, err)
		withError = true
		return noErr
	}

	md = metadata.Pairs("authorization", resp.Token)
	ctx = metadata.NewOutgoingContext(context.Background(), md)

	a.logger.Infof("audit #%d - auditing database %s\n", a.index, dbName)
	a.databaseIndex++

	state, err = a.serviceClient.CurrentState(ctx, &empty.Empty{})
	if err != nil {
		a.logger.Errorf("error getting current state: %v", err)
		withError = true
		return noErr
	}

	if a.serverSigningPubKey != nil {
		if okSig, err := state.CheckSignature(a.serverSigningPubKey); err != nil || !okSig {
			a.logger.Errorf(
				"audit #%d aborted: could not verify signature on server state at %s @ %s",
				a.index, serverID, a.serverAddress)
			withError = true
			return noErr
		}
	}

	isEmptyDB := state.TxId == 0

	serverID = a.getServerID(ctx)
	prevState, err = a.history.Get(serverID, dbName)
	if err != nil {
		a.logger.Errorf(err.Error())
		withError = true
		return noErr
	}

	if prevState != nil {
		if isEmptyDB {
			a.logger.Errorf(
				"audit #%d aborted: database is empty on server %s @ %s, "+
					"but locally a previous state exists with hash %x at id %d",
				a.index, serverID, a.serverAddress, prevState.TxHash, prevState.TxId)
			withError = true
			return noErr
		}

		vtx, err := a.serviceClient.VerifiableTxById(ctx, &schema.VerifiableTxRequest{
			Tx:           state.TxId,
			ProveSinceTx: prevState.TxId,
		})
		if err != nil {
			a.logger.Errorf(
				"error fetching consistency proof for previous state %d: %v",
				prevState.TxId, err)
			withError = true
			return noErr
		}

		verified = store.VerifyDualProof(
			schema.DualProofFrom(vtx.DualProof),
			prevState.TxId,
			state.TxId,
			schema.DigestFrom(prevState.TxHash),
			schema.DigestFrom(state.TxHash),
		)

		a.logger.Infof("audit #%d result:\n db: %s, consistent:	%t\n"+
			"  previous state:	%x at tx: %d\n  current state:	%x at tx: %d",
			a.index, dbName, verified,
			prevState.TxHash, prevState.TxId, state.TxHash, state.TxId)

		checked = true
		// publish audit notification
		if len(a.notificationConfig.URL) > 0 {
			err := a.publishAuditNotification(
				dbName,
				time.Now(),
				!verified,
				&State{
					Tx:   prevState.TxId,
					Hash: fmt.Sprintf("%x", prevState.TxHash),
					Signature: Signature{
						Signature: base64.StdEncoding.EncodeToString(prevState.GetSignature().GetSignature()),
						PublicKey: base64.StdEncoding.EncodeToString(prevState.GetSignature().GetPublicKey()),
					},
				},
				&State{
					Tx:   state.TxId,
					Hash: fmt.Sprintf("%x", state.TxHash),
					Signature: Signature{
						Signature: base64.StdEncoding.EncodeToString(state.GetSignature().GetSignature()),
						PublicKey: base64.StdEncoding.EncodeToString(state.GetSignature().GetPublicKey()),
					},
				},
			)
			if err != nil {
				a.logger.Errorf(
					"error publishing audit notification for db %s: %v", dbName, err)
			} else {
				a.logger.Infof(
					"audit notification for db %s has been published at %s",
					dbName, a.notificationConfig.URL)
			}
		}
	} else if isEmptyDB {
		a.logger.Warningf("audit #%d canceled: database is empty on server %s @ %s",
			a.index, serverID, a.serverAddress)
		return noErr
	}

	if !verified {
		a.logger.Warningf(
			"audit #%d detected possible tampering of db %s remote state (at id %d) "+
				"so it will not overwrite the previous local state (at id %d)",
			a.index, dbName, state.TxId, prevState.TxId)
	} else if prevState == nil || state.TxId != prevState.TxId {
		if err := a.history.Set(serverID, dbName, state); err != nil {
			a.logger.Errorf(err.Error())
			return noErr
		}
	}
	a.logger.Infof("audit #%d finished in %s @ %s",
		a.index, time.Since(start), time.Now().Format(time.RFC3339Nano))

	return noErr
}

// Signature ...
type Signature struct {
	Signature string `json:"signature"`
	PublicKey string `json:"public_key"`
}

// State ...
type State struct {
	Tx        uint64    `json:"tx" validate:"required"`
	Hash      string    `json:"hash" validate:"required"`
	Signature Signature `json:"signature" validate:"required"`
}

// AuditNotificationRequest ...
type AuditNotificationRequest struct {
	Username      string    `json:"username" validate:"required"`
	Password      string    `json:"password" validate:"required"`
	DB            string    `json:"db" validate:"required"`
	RunAt         time.Time `json:"run_at" validate:"required" example:"2020-11-13T00:53:42+01:00"`
	Tampered      bool      `json:"tampered"`
	PreviousState *State    `json:"previous_state"`
	CurrentState  *State    `json:"current_state"`
}

func (a *defaultAuditor) publishAuditNotification(
	db string,
	runAt time.Time,
	tampered bool,
	prevState *State,
	currState *State) error {

	payload := AuditNotificationRequest{
		Username:      a.notificationConfig.Username,
		Password:      a.notificationConfig.Password,
		DB:            db,
		RunAt:         runAt,
		Tampered:      tampered,
		PreviousState: prevState,
		CurrentState:  currState,
	}

	reqBody, err := json.Marshal(payload)
	if err != nil {
		return err
	}

	req, err := http.NewRequest("POST", a.notificationConfig.URL, bytes.NewBuffer(reqBody))
	if err != nil {
		return err
	}

	req.Header.Set("Content-Type", "application/json")

	resp, err := a.notificationConfig.publishFunc(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	payload.Password = ""

	switch resp.StatusCode {
	case http.StatusOK, http.StatusCreated, http.StatusAccepted, http.StatusNoContent:
	default:
		respBody, _ := ioutil.ReadAll(resp.Body)
		return fmt.Errorf(
			"POST %s request with payload %+v: "+
				"got unexpected response status %s with response body %s",
			a.notificationConfig.URL, payload,
			resp.Status, respBody)
	}

	return nil
}

func (a *defaultAuditor) getServerID(ctx context.Context) string {
	serverID, err := a.uuidProvider.CurrentUUID(ctx)
	if err != nil {
		if err != state.ErrNoServerUuid {
			a.logger.Errorf("error getting server UUID: %v", err)
		} else {
			a.logger.Warningf(err.Error())
		}
	}

	if serverID == "" {
		serverID = strings.ReplaceAll(
			strings.ReplaceAll(a.serverAddress, ".", "-"),
			":", "_")
		serverID = a.slugifyRegExp.ReplaceAllString(serverID, "")
		a.logger.Debugf(
			"the current immudb server @ %s will be identified as %s",
			a.serverAddress, serverID)
	}

	return serverID
}

// repeat executes f every interval until stopc is closed or f returns an error.
// It executes f once right after being called.
func repeat(
	interval time.Duration,
	stopc <-chan struct{},
	f func() error,
) error {
	tick := time.NewTicker(interval)
	defer tick.Stop()

	for {
		if err := f(); err != nil {
			return err
		}

		select {
		case <-stopc:
			return nil
		case <-tick.C:
		}
	}

}

/*
Copyright 2019-2020 vChain, Inc.

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
	"io/ioutil"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/codenotary/immudb/pkg/auth"
	"github.com/codenotary/immudb/pkg/client"
	"github.com/codenotary/immudb/pkg/client/timestamp"
	"github.com/codenotary/immudb/pkg/logger"
	"github.com/codenotary/immudb/pkg/server"
	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes/empty"
	"github.com/grpc-ecosystem/grpc-gateway/runtime"
	"google.golang.org/grpc"
	"google.golang.org/grpc/status"
)

type Auditor interface {
	Run(interval time.Duration, stopc <-chan struct{}, donec chan<- struct{})
}

type auditor struct {
	index         uint64
	logger        logger.Logger
	dir           string
	immudbAddress string
	dialOptions   []grpc.DialOption
	ts            client.TimestampService
	username      []byte
	password      []byte
	slugifex      *regexp.Regexp
}

func DefaultAuditor(
	options *client.Options,
	interval time.Duration,
	username string,
	password string,
) (Auditor, error) {
	logr := logger.NewSimpleLogger("auditor", os.Stderr)
	dir := filepath.Join(options.Dir, "auditor")
	if err := os.MkdirAll(dir, os.ModePerm); err != nil {
		return nil, err
	}
	dt, err := timestamp.NewTdefault()
	if err != nil {
		return nil, err
	}
	slugifex, err := regexp.Compile(`[^a-zA-Z0-9\-_]+`)
	if err != nil {
		logr.Warningf("error compiling regex for slugifier: %v", err)
	}
	return &auditor{
		0,
		logr,
		dir,
		options.Address + ":" + strconv.Itoa(options.Port),
		*options.DialOptions,
		client.NewTimestampService(dt),
		[]byte(username),
		[]byte(password),
		slugifex,
	}, nil
}

func (a *auditor) Run(
	interval time.Duration,
	stopc <-chan struct{},
	donec chan<- struct{},
) {
	defer func() { donec <- struct{}{} }()
	a.logger.Infof("starting auditor with a %s interval ...", interval)
	err := repeat(interval, stopc, a.audit)
	if err != nil {
		return
	}
	a.logger.Infof("auditor stopped")
}

func (a *auditor) audit() error {
	start := time.Now()
	a.index++
	a.logger.Infof("audit #%d started @ %s", a.index, start)

	// returning an error would completely stop the auditor process
	var noErr error

	ctx := context.Background()
	conn, err := a.connect(ctx)
	if err != nil {
		return noErr
	}
	defer a.closeConnection(conn)
	serviceClient := schema.NewImmuServiceClient(conn)

	rootsDir := filepath.Join(a.dir, a.getServerID(ctx, serviceClient))
	if err = os.MkdirAll(rootsDir, os.ModePerm); err != nil {
		a.logger.Errorf("error creating roots dir %s: %v", rootsDir, err)
		return noErr
	}

	roots, err := ioutil.ReadDir(rootsDir)
	if err != nil {
		a.logger.Errorf("error reading roots dir %s: %v", rootsDir, err)
		return noErr
	}

	var prevRoot *schema.Root
	var root *schema.Root
	verified := true
	if len(roots) > 0 {
		prevRootFilename := filepath.Join(rootsDir, roots[len(roots)-1].Name())
		prevRootBytes, err := ioutil.ReadFile(prevRootFilename)
		if err != nil {
			a.logger.Errorf(
				"error reading previous root from %s: %v", prevRootFilename, err)
			return noErr
		}
		prevRoot = new(schema.Root)
		if err = proto.Unmarshal(prevRootBytes, prevRoot); err != nil {
			a.logger.Errorf(
				"error unmarshaling previous root from %s: %v", prevRootFilename, err)
			return noErr
		}
		proof, err := serviceClient.Consistency(ctx, &schema.Index{
			Index: prevRoot.GetIndex(),
		})
		if err != nil {
			a.logger.Errorf(
				"error fetching consistency proof for previous root %d (from %s): %v",
				prevRoot.GetIndex(), prevRootFilename, err)
			return noErr
		}
		verified =
			proof.Verify(schema.Root{Index: prevRoot.Index, Root: prevRoot.Root})
		a.logger.Infof("consistency check result:\n"+
			"  consistent:	%t\n  firstRoot:	%x at index: %d\n  secondRoot:	%x at index: %d",
			verified, proof.FirstRoot, proof.First, proof.SecondRoot, proof.Second)
		root = &schema.Root{Index: proof.Second, Root: proof.SecondRoot}
	} else {
		root, err = serviceClient.CurrentRoot(ctx, &empty.Empty{})
		if err != nil {
			a.logger.Errorf("error getting current root: %v", err)
			return noErr
		}
	}

	if verified {
		if prevRoot == nil ||
			root.GetIndex() != prevRoot.GetIndex() ||
			!bytes.Equal(root.Root, prevRoot.Root) {
			rootBytes, err := proto.Marshal(root)
			if err != nil {
				a.logger.Errorf("error marshaling root %d: %v", root.GetIndex(), err)
				return noErr
			}
			rootFilename := filepath.Join(rootsDir, ".root")
			if err = ioutil.WriteFile(rootFilename, rootBytes, 0644); err != nil {
				a.logger.Errorf(
					"error writing root %d to file %s: %v",
					root.GetIndex(), rootFilename, err)
				return noErr
			}
		}
	} else {
		a.logger.Warningf(
			"audit #%d detected possible tampering of remote root (at index %d) "+
				"so it will not overwrite the previous local root (at index %d)",
			a.index, root.GetIndex(), prevRoot.GetIndex())
	}
	a.logger.Infof(
		"audit #%d finished in %s @ %s", a.index, time.Since(start), time.Now())
	return noErr
}

func (a *auditor) connect(ctx context.Context) (*grpc.ClientConn, error) {
	conn, err := grpc.Dial(a.immudbAddress, a.dialOptions...)
	if err != nil {
		a.logger.Errorf("error dialing (pre-login) to immudb @ %s: %v", a.immudbAddress, err)
		return nil, err
	}
	defer a.closeConnection(conn)
	serviceClient := schema.NewImmuServiceClient(conn)
	loginResponse, err := serviceClient.Login(ctx, &schema.LoginRequest{
		User:     a.username,
		Password: a.password,
	})
	if err != nil {
		grpcStatus, ok1 := status.FromError(err)
		authDisabled, ok2 := status.FromError(auth.ErrServerAuthDisabled)
		if !ok1 || !ok2 || grpcStatus.Code() != authDisabled.Code() ||
			grpcStatus.Message() != authDisabled.Message() {
			a.logger.Errorf("error logging in: %v", err)
			return nil, err
		}
	}
	if loginResponse != nil {
		token := string(loginResponse.GetToken())
		a.dialOptions = append(a.dialOptions,
			grpc.WithUnaryInterceptor(auth.ClientUnaryInterceptor(token)))
	}
	connWithToken, err := grpc.Dial(a.immudbAddress, a.dialOptions...)
	if err != nil {
		a.logger.Errorf(
			"error dialing to immudb @ %s: %v", a.immudbAddress, err)
		return nil, err
	}
	return connWithToken, nil
}

func (a *auditor) getServerID(
	ctx context.Context,
	serviceClient schema.ImmuServiceClient,
) string {
	var serverID string
	var metadata runtime.ServerMetadata
	_, err := serviceClient.Health(
		ctx,
		new(empty.Empty),
		grpc.Header(&metadata.HeaderMD),
		grpc.Trailer(&metadata.TrailerMD),
	)
	if err != nil {
		a.logger.Errorf("health error: %v", err)
	} else if len(metadata.HeaderMD.Get(server.SERVER_UUID_HEADER)) > 0 {
		serverID = metadata.HeaderMD.Get(server.SERVER_UUID_HEADER)[0]
	}
	if serverID == "" {
		serverID = strings.ReplaceAll(
			strings.ReplaceAll(a.immudbAddress, ".", "-"),
			":", "_")
		serverID = a.slugifex.ReplaceAllString(serverID, "")
		a.logger.Debugf(
			"%s server UUID header is not provided by immudb; auditor will use "+
				"the immudb url+port slugified as %s to identify the immudb server",
			server.SERVER_UUID_HEADER, serverID)
	}
	return serverID
}

func (a *auditor) closeConnection(conn *grpc.ClientConn) {
	if err := conn.Close(); err != nil {
		a.logger.Errorf("error closing connection: %v", err)
	}
}

// repeat executes f every interval seconds until stopc is closed or f returns an error.
// It executes f once right after being called.
func repeat(interval time.Duration, stopc <-chan struct{}, f func() error) error {
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

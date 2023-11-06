package client

import (
	"context"
	"fmt"

	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/codenotary/immudb/pkg/client/cache"
	"github.com/codenotary/immudb/pkg/client/errors"
	"github.com/codenotary/immudb/pkg/client/state"
	"github.com/codenotary/immudb/pkg/signer"
	"github.com/codenotary/immudb/pkg/stream"
	"github.com/golang/protobuf/ptypes/empty"
	"google.golang.org/grpc"
)

// OpenSession establishes a new session with the server, this method also opens new
// connection to the server.
//
// Note: it is important to call CloseSession() once the session is no longer needed.
func (c *immuClient) OpenSession(ctx context.Context, user []byte, pass []byte, database string) (err error) {
	if c.IsConnected() {
		return errors.FromError(ErrSessionAlreadyOpen)
	}

	if c.Options.ServerSigningPubKey != "" {
		pk, e := signer.ParsePublicKeyFile(c.Options.ServerSigningPubKey)
		if e != nil {
			return e
		}
		c.WithServerSigningPubKey(pk)
	}

	if c.Options.StreamChunkSize < stream.MinChunkSize {
		return errors.New(stream.ErrChunkTooSmall).WithCode(errors.CodInvalidParameterValue)
	}

	dialOptions := c.SetupDialOptions(c.Options)

	clientConn, err := grpc.Dial(c.Options.Bind(), dialOptions...)
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			_ = clientConn.Close()
		}
	}()

	serviceClient := schema.NewImmuServiceClient(clientConn)
	resp, err := serviceClient.OpenSession(ctx, &schema.OpenSessionRequest{
		Username:     user,
		Password:     pass,
		DatabaseName: database,
	})
	if err != nil {
		return errors.FromError(err)
	}
	defer func() {
		if err != nil {
			_, _ = serviceClient.CloseSession(ctx, new(empty.Empty))
		}
	}()

	stateCache := cache.NewFileCache(c.Options.Dir)
	stateProvider := state.NewStateProvider(serviceClient)

	stateService, err := state.NewStateServiceWithUUID(stateCache, c.Logger, stateProvider, resp.GetServerUUID())
	if err != nil {
		return errors.FromError(fmt.Errorf("unable to create state service: %v", err))
	}

	if !c.Options.DisableIdentityCheck {
		stateService.SetServerIdentity(c.getServerIdentity())
	}

	c.clientConn = clientConn
	c.ServiceClient = serviceClient
	c.Options.DialOptions = dialOptions
	c.SessionID = resp.GetSessionID()

	c.HeartBeater = NewHeartBeater(c.SessionID, c.ServiceClient, c.Options.HeartBeatFrequency, c.errorHandler, c.Logger)
	c.HeartBeater.KeepAlive(context.Background())

	c.WithStateService(stateService)

	c.Options.CurrentDatabase = database

	return nil
}

// CloseSession closes the current session and the connection to the server,
// this call also allows the server to free up all resources allocated for a session
// (without explicit call, the server will only free resources after session inactivity timeout).
func (c *immuClient) CloseSession(ctx context.Context) error {
	if !c.IsConnected() {
		return errors.FromError(ErrNotConnected)
	}

	defer func() {
		c.SessionID = ""
		c.clientConn = nil
		c.ServiceClient = nil
		c.StateService = nil
		c.serverSigningPubKey = nil
		c.HeartBeater = nil
	}()

	c.HeartBeater.Stop()

	defer c.clientConn.Close()

	_, err := c.ServiceClient.CloseSession(ctx, new(empty.Empty))
	if err != nil {
		return errors.FromError(err)
	}

	return nil
}

// GetSessionID returns the current internal session identifier.
func (c *immuClient) GetSessionID() string {
	return c.SessionID
}

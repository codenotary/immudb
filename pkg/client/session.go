package client

import (
	"context"
	"fmt"

	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/codenotary/immudb/pkg/client/cache"
	"github.com/codenotary/immudb/pkg/client/errors"
	"github.com/codenotary/immudb/pkg/client/heartbeater"
	"github.com/codenotary/immudb/pkg/client/state"
	"github.com/codenotary/immudb/pkg/signer"
	"github.com/codenotary/immudb/pkg/stream"
	"github.com/golang/protobuf/ptypes/empty"
	"google.golang.org/grpc"
)

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

	stateProvider := state.NewStateProvider(serviceClient)

	stateService, err := state.NewStateServiceWithUUID(cache.NewFileCache(c.Options.Dir), c.Logger, stateProvider, resp.GetServerUUID())
	if err != nil {
		return errors.FromError(fmt.Errorf("unable to create state service: %v", err))
	}

	c.clientConn = clientConn
	c.ServiceClient = serviceClient
	c.Options.DialOptions = dialOptions
	c.SessionID = resp.GetSessionID()

	c.HeartBeater = heartbeater.NewHeartBeater(c.SessionID, c.ServiceClient, c.Options.HeartBeatFrequency)
	c.HeartBeater.KeepAlive(context.Background())

	c.WithStateService(stateService)

	c.Options.CurrentDatabase = database
	if c.credentials != nil { // @TODO: Thread safety concerns
		return nil
	}

	c.credentials = &creds{
		user:     user,
		pass:     pass,
		database: database,
	}

	return nil
}

func (c *immuClient) CloseSession(ctx context.Context) error {
	if !c.IsConnected() {
		return errors.FromError(ErrNotConnected)
	}

	defer func() {
		c.SessionID = ""
		c.clientConn = nil
	}()

	c.HeartBeater.Stop()

	defer c.clientConn.Close()

	_, err := c.ServiceClient.CloseSession(ctx, new(empty.Empty))
	if err != nil {
		return errors.FromError(err)
	}

	return nil
}

func (c *immuClient) GetSessionID() string {
	return c.SessionID
}

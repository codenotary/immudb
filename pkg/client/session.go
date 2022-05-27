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

	c.Options.DialOptions = c.SetupDialOptions(c.Options)

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

	if c.clientConn, err = grpc.Dial(c.Options.Bind(), c.Options.DialOptions...); err != nil {
		return err
	}

	c.ServiceClient = schema.NewImmuServiceClient(c.clientConn)

	resp, err := c.ServiceClient.OpenSession(ctx, &schema.OpenSessionRequest{
		Username:     user,
		Password:     pass,
		DatabaseName: database,
	})
	if err != nil {
		return errors.FromError(err)
	}

	c.SessionID = resp.GetSessionID()

	c.HeartBeater = heartbeater.NewHeartBeater(resp.GetSessionID(), c.ServiceClient, c.Options.HeartBeatFrequency)
	c.HeartBeater.KeepAlive(ctx)

	stateProvider := state.NewStateProvider(c.ServiceClient)

	stateService, err := state.NewStateServiceWithUUID(cache.NewFileCache(c.Options.Dir), c.Logger, stateProvider, resp.GetServerUUID())
	if err != nil {
		return errors.FromError(fmt.Errorf("unable to create state service: %v", err))
	}

	c.WithStateService(stateService)

	c.Options.CurrentDatabase = database

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

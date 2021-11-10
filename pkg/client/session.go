package client

import (
	"context"
	"fmt"
	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/codenotary/immudb/pkg/client/cache"
	"github.com/codenotary/immudb/pkg/client/errors"
	"github.com/codenotary/immudb/pkg/client/state"
	"google.golang.org/grpc"
)

func (c *immuClient) OpenSession(ctx context.Context, user []byte, pass []byte, database string) (serverUUID string, sessionID string, err error) {
	if c.clientConn, err = grpc.Dial(c.Options.Bind(), c.Options.DialOptions...); err != nil {
		return "", "", err
	}

	serviceClient := schema.NewImmuServiceClient(c.clientConn)
	// todo remove WithServiceClient
	c.WithServiceClient(serviceClient)

	resp, err := c.ServiceClient.OpenSession(ctx, &schema.OpenSessionRequest{
		User:         user,
		Password:     pass,
		DatabaseName: database,
	})
	if err != nil {
		return "", "", errors.FromError(err)
	}

	err = c.Tkns.SetToken(database, resp.GetSessionID())
	if err != nil {
		return "", "", errors.FromError(err)
	}

	stateProvider := state.NewStateProvider(serviceClient)

	stateService, err := state.NewStateServiceWithUUID(cache.NewFileCache(c.Options.Dir), c.Logger, stateProvider, resp.GetServerUUID())
	if err != nil {
		return "", "", errors.FromError(fmt.Errorf("unable to create state service: %v", err))
	}

	c.WithStateService(stateService)

	return resp.GetServerUUID(), resp.GetSessionID(), nil
}

func (c *immuClient) CloseSession(ctx context.Context) error {
	if !c.IsConnected() {
		return errors.FromError(ErrNotConnected)
	}

	sessIDExists, err := c.Tkns.IsTokenPresent()
	if err != nil {
		return errors.FromError(err)
	}

	if !sessIDExists {
		errors.New("no session open")
	}

	sessionID, err := c.Tkns.GetToken()
	if err != nil {
		return errors.FromError(err)
	}

	_, err = c.ServiceClient.CloseSession(ctx, &schema.CloseSessionRequest{SessionID: sessionID})
	if err = c.Tkns.DeleteToken(); err != nil {
		return errors.FromError(fmt.Errorf("error clearing sessionID when closing session: %v", err))
	}

	return nil
}

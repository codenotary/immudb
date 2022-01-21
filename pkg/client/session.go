package client

import (
	"context"
	"fmt"

	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/codenotary/immudb/pkg/client/cache"
	"github.com/codenotary/immudb/pkg/client/errors"
	"github.com/codenotary/immudb/pkg/client/heartbeater"
	"github.com/codenotary/immudb/pkg/client/sessionservice"
	"github.com/codenotary/immudb/pkg/client/state"
	"github.com/codenotary/immudb/pkg/signer"
	"github.com/codenotary/immudb/pkg/stream"
	"github.com/golang/protobuf/ptypes/empty"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

func (c *immuClient) OpenSessionNew(ctx context.Context, user []byte, pass []byte, database string) (*sessionservice.Session, error) {
	if c.serverSigningPubKey == nil && c.Options.ServerSigningPubKey != "" {
		pk, e := signer.ParsePublicKeyFile(c.Options.ServerSigningPubKey)
		if e != nil {
			return nil, e
		}
		c.WithServerSigningPubKey(pk)
	}

	if c.Options.StreamChunkSize < stream.MinChunkSize {
		return nil, errors.New(stream.ErrChunkTooSmall).WithCode(errors.CodInvalidParameterValue)
	}

	if c.clientConn == nil {
		c.Options.DialOptions = c.SetupDialOptions(c.Options)
		var err error
		if c.clientConn, err = grpc.Dial(c.Options.Bind(), c.Options.DialOptions...); err != nil {
			return nil, err
		}

		if c.ServiceClient == nil {
			c.ServiceClient = schema.NewImmuServiceClient(c.clientConn)
		}
	}

	resp, err := c.ServiceClient.OpenSession(ctx, &schema.OpenSessionRequest{
		Username:     user,
		Password:     pass,
		DatabaseName: database,
	})
	if err != nil {
		return nil, errors.FromError(err)
	}

	s := sessionservice.Session{
		SessionID: resp.GetSessionID(),
		Database:  database,
		Hearbeat:  heartbeater.NewHeartBeater(resp.GetSessionID(), c.ServiceClient, c.Options.HeartBeatFrequency),
	}

	s.Hearbeat.KeepAlive(metadata.AppendToOutgoingContext(context.Background(), "sessionid", s.SessionID))
	err = c.SessionService.AddSession(s)
	if err != nil {
		return nil, err
	}

	if c.StateService == nil {
		stateProvider := state.NewStateProvider(c.ServiceClient)
		stateService, err := state.NewStateServiceWithUUID(cache.NewFileCache(c.Options.Dir), c.Logger, stateProvider, resp.GetServerUUID())
		if err != nil {
			return &s, errors.FromError(fmt.Errorf("unable to create state service: %v", err))
		}

		c.WithStateService(stateService)
	}

	return &s, nil
}

func (c *immuClient) OpenSession(ctx context.Context, user []byte, pass []byte, database string) (err error) {
	if c.SessionService.Len() > 0 {
		return ErrSessionAlreadyOpen
	}

	if _, err := c.OpenSessionNew(ctx, user, pass, database); err != nil {
		return err
	}

	return nil
}

func (c *immuClient) CloseSession(ctx context.Context) error {
	if !c.IsConnected() {
		return errors.FromError(ErrNotConnected)
	}

	var err error
	var session sessionservice.Session

	session, err = c.SessionService.SessionFromCtx(ctx)
	if err != nil && c.SessionService.Len() > 1 {
		return fmt.Errorf("session not found in context")
	}

	if session.SessionID == "" && c.SessionService.Len() == 1 {
		session, err = c.SessionService.GetSession("")
		if err != nil {
			return fmt.Errorf("session not found")
		}
	}

	session.Hearbeat.Stop()

	_, err = c.ServiceClient.CloseSession(ctx, new(empty.Empty))
	if err != nil {
		return errors.FromError(err)
	}
	c.SessionService.DeleteSession(session.SessionID)

	return nil
}

// func (c *immuClient) GetSessionID() string {
// 	return c.SessionID
// }

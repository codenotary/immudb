package state

import (
	"context"

	"github.com/codenotary/immudb/pkg/api/schema"
)

type SessionProvider interface {
	OpenSession(ctx context.Context) (*schema.OpenSessionResponse, error)
}

type sessionProvider struct {
	client schema.ImmuServiceClient
}

func NewSessionProvider(client schema.ImmuServiceClient) SessionProvider {
	return &sessionProvider{client}
}

func (r *sessionProvider) OpenSession(ctx context.Context) (*schema.OpenSessionResponse, error) {
	var protoReq = schema.OpenSessionRequest{
		Username:     []byte(""),
		Password:     []byte(""),
		DatabaseName: "",
	}
	return r.client.OpenSession(ctx, &protoReq)
}

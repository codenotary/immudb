package auth

import (
	"context"
	"google.golang.org/grpc/metadata"
)

type AuthType int

const (
	TokenAuth AuthType = iota
	SessionAuth
	None
)

func GetAuthTypeFromContext(ctx context.Context) AuthType {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return None
	}
	authHeader, ok := md["sessionid"]
	if ok && len(authHeader) >= 1 {
		return SessionAuth
	}
	authHeader, ok = md["authorization"]
	if ok && len(authHeader) >= 1 {
		return TokenAuth
	}
	return None
}

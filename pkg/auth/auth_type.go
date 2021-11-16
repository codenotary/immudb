package auth

import (
	"context"
	"google.golang.org/grpc/metadata"
)

type AUTH_TYPE int

const (
	TOKEN_AUTH AUTH_TYPE = iota
	SESSION_AUTH
	NONE
)

func GetAuthTypeFromContext(ctx context.Context) AUTH_TYPE {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return NONE
	}
	authHeader, ok := md["sessionid"]
	if ok && len(authHeader) >= 1 {
		return SESSION_AUTH
	}
	authHeader, ok = md["authorization"]
	if ok && len(authHeader) >= 1 {
		return TOKEN_AUTH
	}
	return NONE
}

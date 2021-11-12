package session

import (
	"context"
	"github.com/codenotary/immudb/pkg/errors"
	"google.golang.org/grpc/metadata"
	"strings"
	"time"
)

type Session struct {
	User               []byte
	Database           string
	CreationTime       time.Time
	LastActivityTime   time.Time
	OngoingTransaction bool
}

func GetSessionID(ctx context.Context) (string, error) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return "", ErrNoSessionIDPresent.WithCode(errors.CodInvalidSessionID)
	}
	authHeader, ok := md["authorization"]
	if !ok || len(authHeader) < 1 {
		return "", ErrNoSessionIDPresent.WithCode(errors.CodInvalidSessionID)
	}
	sessionID := strings.TrimPrefix(authHeader[0], "Bearer ")
	if sessionID == "" {
		return "", ErrNoSessionIDPresent.WithCode(errors.CodInvalidSessionID)
	}
	return sessionID, nil
}

package server

import (
	"context"
	"time"

	"google.golang.org/grpc"
)

func (s *ImmuServer) AccessLogStreamInterceptor(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	start := time.Now()

	err := handler(srv, ss)

	_ = s.logAccess(ss.Context(), info.FullMethod, time.Since(start), err)
	return err
}

func (s *ImmuServer) AccessLogInterceptor(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
	start := time.Now()

	res, err := handler(ctx, req)

	_ = s.logAccess(ctx, info.FullMethod, time.Since(start), err)
	return res, err
}

func (s *ImmuServer) logAccess(ctx context.Context, method string, rpcDuration time.Duration, rpcErr error) error {
	if !s.Options.LogAccess {
		return nil
	}

	var username string

	_, user, err := s.getLoggedInUserdataFromCtx(ctx)
	if err == nil {
		username = user.Username
	}

	ip := ipAddrFromContext(ctx)

	// Pass rpcDuration directly and use %v so fmt invokes Duration.String()
	// only inside the logger's level-gated Printf — saves one allocation per
	// RPC when LogAccess is enabled but the underlying log level is above Info.
	if rpcErr == nil {
		s.Logger.Infof("user=%s,ip=%s,method=%s,duration=%v", username, ip, method, rpcDuration)
	} else {
		s.Logger.Infof("user=%s,ip=%s,method=%s,duration=%v,error=%s", username, ip, method, rpcDuration, rpcErr)
	}
	return nil
}

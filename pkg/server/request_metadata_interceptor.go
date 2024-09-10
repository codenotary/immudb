package server

import (
	"context"
	"strings"

	"github.com/codenotary/immudb/pkg/api/schema"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
)

func (s *ImmuServer) InjectRequestMetadataUnaryInterceptor(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
	if !s.Options.LogRequestMetadata {
		return handler(ctx, req)
	}
	return handler(s.withRequestMetadata(ctx), req)
}

func (s *ImmuServer) InjectRequestMetadataStreamInterceptor(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	ctx := ss.Context()
	if !s.Options.LogRequestMetadata {
		return handler(srv, ss)
	}
	return handler(srv, &serverStreamWithContext{ServerStream: ss, ctx: s.withRequestMetadata(ctx)})
}

type serverStreamWithContext struct {
	grpc.ServerStream
	ctx context.Context
}

func (s *serverStreamWithContext) Context() context.Context {
	return s.ctx
}

func (s *ImmuServer) withRequestMetadata(ctx context.Context) context.Context {
	if !s.Options.LogRequestMetadata {
		return ctx
	}

	_, user, err := s.getLoggedInUserdataFromCtx(ctx)
	if err != nil {
		return ctx
	}

	md := schema.Metadata{
		schema.UserRequestMetadataKey: user.Username,
	}

	ip := ipAddrFromContext(ctx)
	if len(ip) > 0 {
		md[schema.IpRequestMetadataKey] = ip
	}
	return schema.ContextWithMetadata(ctx, md)
}

func ipAddrFromContext(ctx context.Context) string {
	md, ok := metadata.FromIncomingContext(ctx)
	if ok {
		// check for the headers forwarded by GRPC-gateway
		if xffValues, ok := md["x-forwarded-for"]; ok && len(xffValues) > 0 {
			return xffValues[0]
		} else if xriValues, ok := md["x-real-ip"]; ok && len(xriValues) > 0 {
			return xriValues[0]
		}
	}

	p, ok := peer.FromContext(ctx)
	if !ok {
		return ""
	}

	addr := p.Addr.String()
	i := strings.Index(addr, ":")
	if i < 0 {
		return addr
	}
	return addr[:i]
}

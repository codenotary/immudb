/*
Copyright 2021 CodeNotary, Inc. All rights reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

	http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package auth

import (
	"context"

	"google.golang.org/grpc/metadata"

	"google.golang.org/grpc"
)

// ClientStreamInterceptor gRPC client interceptor for streams
func ClientStreamInterceptor(token string) func(context.Context, *grpc.StreamDesc, *grpc.ClientConn, string, grpc.Streamer, ...grpc.CallOption) (grpc.ClientStream, error) {
	return func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
		opts = append(opts, grpc.PerRPCCredentials(TokenAuthStruct{
			Token: token,
		}))
		return streamer(ctx, desc, cc, method, opts...)
	}
}

// ClientUnaryInterceptor gRPC client interceptor for unary methods
func ClientUnaryInterceptor(token string) func(context.Context, string, interface{}, interface{}, *grpc.ClientConn, grpc.UnaryInvoker, ...grpc.CallOption) error {
	return func(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		opts = append(opts, grpc.PerRPCCredentials(TokenAuthStruct{
			Token: token,
		}))
		return invoker(ctx, method, req, reply, cc, opts...)
	}
}

// TokenAuthStruct authentication token data structure
type TokenAuthStruct struct {
	Token string
}

// GetRequestMetadata callback which returns the Bearer token to be set in request metadata
func (t TokenAuthStruct) GetRequestMetadata(ctx context.Context, in ...string) (map[string]string, error) {
	var token string
	if md, ok := metadata.FromOutgoingContext(ctx); ok && len(md.Get("authorization")) > 0 {
		token = md.Get("authorization")[0]
	} else {
		token = t.Token
	}
	return map[string]string{
		"authorization": "Bearer " + token,
	}, nil
}

// RequireTransportSecurity callback which returns whether TLS is mandatory or not
func (TokenAuthStruct) RequireTransportSecurity() bool {
	return false
}

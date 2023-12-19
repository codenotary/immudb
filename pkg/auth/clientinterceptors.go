/*
Copyright 2024 Codenotary Inc. All rights reserved.

SPDX-License-Identifier: BUSL-1.1
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    https://mariadb.com/bsl11/

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package auth

import (
	"context"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

// ClientStreamInterceptor gRPC client interceptor for streams
func ClientStreamInterceptor(token string) func(context.Context, *grpc.StreamDesc, *grpc.ClientConn, string, grpc.Streamer, ...grpc.CallOption) (grpc.ClientStream, error) {
	return func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
		ctx = updateAuthHeader(ctx, token)
		return streamer(ctx, desc, cc, method, opts...)
	}
}

// ClientUnaryInterceptor gRPC client interceptor for unary methods
func ClientUnaryInterceptor(token string) func(context.Context, string, interface{}, interface{}, *grpc.ClientConn, grpc.UnaryInvoker, ...grpc.CallOption) error {
	return func(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		ctx = updateAuthHeader(ctx, token)
		return invoker(ctx, method, req, reply, cc, opts...)
	}
}

// updateAuthHeader ensures the grpc metadata in the context contains the correct
// authorization header value. The token may be either taken from the client
// object where it is managed by a token service through the `token` argument,
// or it can be given in the metadata inside the context.
func updateAuthHeader(ctx context.Context, token string) context.Context {
	if md, ok := metadata.FromOutgoingContext(ctx); ok && len(md.Get("authorization")) > 0 {
		// The token provided through the metadata has a higher priority than the
		// one set for the whole client - this allows customization of the token
		// per each API call.
		token = md.Get("authorization")[0]
	}

	md, ok := metadata.FromOutgoingContext(ctx)
	if !ok {
		md = metadata.New(nil)
	}
	// The final token value must be provided with the `Bearer ` prefix.
	md.Set("authorization", "Bearer "+token)
	return metadata.NewOutgoingContext(ctx, md)
}

/*
Copyright 2025 Codenotary Inc. All rights reserved.

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

package client

import (
	"context"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

// SessionIDInjectorInterceptor is a gRPC interceptor that inject sessionID into the outgoing context
func (c *immuClient) SessionIDInjectorInterceptor(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
	ctx = c.populateCtx(ctx)
	ris := invoker(ctx, method, req, reply, cc, opts...)
	return ris
}

// SessionIDInjectorInterceptor is a gRPC stream interceptor that inject sessionID into the outgoing context
func (c *immuClient) SessionIDInjectorStreamInterceptor(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
	ctx = c.populateCtx(ctx)
	return streamer(ctx, desc, cc, method, opts...)
}

func (c *immuClient) populateCtx(ctx context.Context) context.Context {
	if c.GetSessionID() != "" {
		ctx = metadata.AppendToOutgoingContext(ctx, "sessionid", c.GetSessionID())
	}
	return ctx
}

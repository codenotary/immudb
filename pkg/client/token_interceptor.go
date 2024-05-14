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

package client

import (
	"context"

	"google.golang.org/grpc/metadata"

	"google.golang.org/grpc"
)

// TokenInterceptor injects authentication token header to outgoing GRPC requests if it has not been set already
func (c *immuClient) TokenInterceptor(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
	ctx, err := c.appendTokenToOutgoingContext(ctx)
	if err != nil {
		return err
	}
	return invoker(ctx, method, req, reply, cc, opts...)
}

// TokenInterceptor injects authentication token header to outgoing GRPC requests if it has not been set already
func (c *immuClient) TokenStreamInterceptor(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
	ctx, err := c.appendTokenToOutgoingContext(ctx)
	if err != nil {
		return nil, err
	}
	return streamer(ctx, desc, cc, method, opts...)
}

func (c *immuClient) appendTokenToOutgoingContext(ctx context.Context) (context.Context, error) {
	if md, ok := metadata.FromOutgoingContext(ctx); !ok || len(md.Get("authorization")) == 0 {
		present, err := c.Tkns.IsTokenPresent()
		if err != nil {
			return nil, err
		}
		if present {
			token, err := c.Tkns.GetToken()
			if err != nil {
				return nil, err
			}
			return metadata.AppendToOutgoingContext(ctx, "authorization", token), nil
		}
	}
	return ctx, nil
}

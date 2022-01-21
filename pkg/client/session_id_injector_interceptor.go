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

package client

import (
	"context"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

// SessionIDInjectorInterceptor inject sessionID or transactionID into the outgoing context
func (c *immuClient) SessionIDInjectorInterceptor(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
	if method != "/immudb.schema.ImmuService/OpenSession" {
		var err error
		ctx, err = c.populateCtx(ctx)
		if err != nil {
			return err
		}
	}

	ris := invoker(ctx, method, req, reply, cc, opts...)
	return ris
}

// ClientStreamInterceptor gRPC client interceptor for streams
func (c *immuClient) SessionIDInjectorStreamInterceptor(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
	if method != "/immudb.schema.ImmuService/OpenSession" {
		var err error
		ctx, err = c.populateCtx(ctx)
		if err != nil {
			return nil, err
		}
	}

	return streamer(ctx, desc, cc, method, opts...)
}

func (c *immuClient) populateCtx(ctx context.Context) (context.Context, error) {
	_, err := c.SessionService.SessionFromCtx(ctx)
	if err != nil {
		// Try to use default single sessionID
		session, err := c.SessionService.GetSession("")
		if err != nil {
			return ctx, err
		}

		ctx = metadata.AppendToOutgoingContext(ctx, "sessionid", session.SessionID)
	}

	return ctx, nil
}

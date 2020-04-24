/*
Copyright 2019-2020 vChain, Inc.

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

	"google.golang.org/grpc"
)

var AuthEnabled bool
var ObserveMetrics func(context.Context)

type WrappedServerStream struct {
	grpc.ServerStream
}

func (w *WrappedServerStream) RecvMsg(m interface{}) error {
	return w.ServerStream.RecvMsg(m)
}

func (w *WrappedServerStream) SendMsg(m interface{}) error {
	return w.ServerStream.SendMsg(m)
}

func ServerStreamInterceptor(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	ctx := ss.Context()
	if AuthEnabled && HasAuth(info.FullMethod) {
		if err := verifyTokenFromCtx(ctx); err != nil {
			return err
		}
	}
	if ObserveMetrics != nil {
		ObserveMetrics(ctx)
	}
	return handler(srv, &WrappedServerStream{ss})
}

func ServerUnaryInterceptor(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
	if AuthEnabled && HasAuth(info.FullMethod) {
		if err := verifyTokenFromCtx(ctx); err != nil {
			return nil, err
		}
	}
	if ObserveMetrics != nil {
		ObserveMetrics(ctx)
	}
	m, err := handler(ctx, req)
	return m, err
}

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
	"strings"

	"google.golang.org/grpc"
	"google.golang.org/grpc/peer"
)

var UpdateMetrics func(context.Context)

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
	if UpdateMetrics != nil {
		UpdateMetrics(ctx)
	}
	if err := checkAuth(ctx, info.FullMethod, srv); err != nil {
		return err
	}
	return handler(srv, &WrappedServerStream{ss})
}

func ServerUnaryInterceptor(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
	if UpdateMetrics != nil {
		UpdateMetrics(ctx)
	}
	if err := checkAuth(ctx, info.FullMethod, req); err != nil {
		return nil, err
	}
	m, err := handler(ctx, req)
	return m, err
}

var localAddress = map[string]bool{
	"127.0.0.1": true,
	"localhost": true,
	"bufconn":   true,
}

func isLocalClient(ctx context.Context) bool {
	isLocal := false
	p, ok := peer.FromContext(ctx)
	if ok && p != nil {
		ipAndPort := strings.Split(p.Addr.String(), ":")
		if len(ipAndPort) > 0 {
			_, isLocal = localAddress[ipAndPort[0]]
		}
	}
	return isLocal
}

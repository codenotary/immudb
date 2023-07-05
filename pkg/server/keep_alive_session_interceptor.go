/*
Copyright 2022 Codenotary Inc. All rights reserved.

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

package server

import (
	"context"

	"github.com/codenotary/immudb/pkg/auth"
	"google.golang.org/grpc"
)

func (s *ImmuServer) KeepALiveSessionStreamInterceptor(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	if auth.GetAuthTypeFromContext(ss.Context()) == auth.SessionAuth {
		_, err := s.KeepAlive(ss.Context(), nil)
		if err != nil {
			return err
		}
	}
	return handler(srv, ss)
}

func (s *ImmuServer) KeepAliveSessionInterceptor(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
	if auth.GetAuthTypeFromContext(ctx) == auth.SessionAuth &&
		info.FullMethod != "/immudb.schema.ImmuService/OpenSession" {
		_, err := s.KeepAlive(ctx, nil)
		if err != nil {
			return nil, err
		}
	}
	return handler(ctx, req)
}

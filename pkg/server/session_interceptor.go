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

package server

import (
	"context"
	"github.com/codenotary/immudb/pkg/session"
	"google.golang.org/grpc"
	"time"
)

func (s *ImmuServer) SessionStreamInterceptor(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	if err := s.setLastActivityTime(ss.Context()); err != nil {
		return err
	}
	return handler(srv, ss)
}

func (s *ImmuServer) SessionInterceptor(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
	if info.FullMethod != "/immudb.schema.ImmuService/OpenSession" {
		if err := s.setLastActivityTime(ctx); err != nil {
			return nil, err
		}
	}
	return handler(ctx, req)
}

func (s *ImmuServer) setLastActivityTime(ctx context.Context) error {
	sessionID, err := session.GetSessionID(ctx)
	if err != nil {
		return err
	}
	s.sessionMux.Lock()
	defer s.sessionMux.Unlock()
	if sess, ok := s.sessions[sessionID]; ok {
		sess.LastActivityTime = time.Now()
	}
	return nil
}

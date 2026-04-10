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

package server

import (
	"context"
	"time"

	"github.com/codenotary/immudb/pkg/audit"
	"github.com/codenotary/immudb/pkg/server/sessions"
	"google.golang.org/grpc"
)

// AuditLogInterceptor captures structured audit events for unary gRPC calls.
func (s *ImmuServer) AuditLogInterceptor(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
	if s.auditLogger == nil {
		return handler(ctx, req)
	}

	start := time.Now()
	res, err := handler(ctx, req)
	s.auditLogger.Log(s.buildAuditEvent(ctx, info.FullMethod, time.Since(start), err))
	return res, err
}

// AuditLogStreamInterceptor captures structured audit events for streaming gRPC calls.
func (s *ImmuServer) AuditLogStreamInterceptor(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	if s.auditLogger == nil {
		return handler(srv, ss)
	}

	start := time.Now()
	err := handler(srv, ss)
	s.auditLogger.Log(s.buildAuditEvent(ss.Context(), info.FullMethod, time.Since(start), err))
	return err
}

func (s *ImmuServer) buildAuditEvent(ctx context.Context, method string, duration time.Duration, rpcErr error) *audit.AuditEvent {
	event := &audit.AuditEvent{
		Timestamp:  time.Now().UnixNano(),
		Method:     method,
		EventType:  audit.ClassifyMethod(method),
		Success:    rpcErr == nil,
		DurationMs: duration.Milliseconds(),
	}

	if rpcErr != nil {
		event.ErrorMsg = rpcErr.Error()
	}

	_, user, err := s.getLoggedInUserdataFromCtx(ctx)
	if err == nil && user != nil {
		event.Username = user.Username
	}

	event.ClientIP = ipAddrFromContext(ctx)

	if sessionID, err := sessions.GetSessionIDFromContext(ctx); err == nil {
		event.SessionID = sessionID
	}

	return event
}

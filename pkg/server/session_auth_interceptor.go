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

package server

import (
	"context"

	"github.com/codenotary/immudb/pkg/auth"
	"github.com/codenotary/immudb/pkg/server/sessions"
	"google.golang.org/grpc"
)

func (s *ImmuServer) SessionAuthInterceptor(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
	if auth.GetAuthTypeFromContext(ctx) == auth.SessionAuth && info.FullMethod != "/immudb.schema.ImmuService/OpenSession" {
		sessionID, err := sessions.GetSessionIDFromContext(ctx)
		if err != nil {
			return nil, err
		}
		if !s.SessManager.SessionPresent(sessionID) {
			return nil, ErrSessionNotFound
		}
	}
	return handler(ctx, req)
}

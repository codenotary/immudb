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
	"strings"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

// UpdateMetrics callback which will be called to update metrics
var UpdateMetrics func(context.Context)

// ServerStreamInterceptor gRPC server interceptor for streams
func ServerStreamInterceptor(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	ctx := ss.Context()
	if UpdateMetrics != nil {
		UpdateMetrics(ctx)
	}
	if IsTampered {
		return status.Errorf(
			codes.DataLoss, "the database should be checked manually as we detected possible tampering")
	}
	if !AuthEnabled {
		if !DevMode {
			if !isLocalClient(ctx) {
				return status.Errorf(
					codes.PermissionDenied,
					"server has authentication disabled: only local connections are accepted")
			}
		}
	}
	return handler(srv, ss)
}

// ServerUnaryInterceptor gRPC server interceptor for unary methods
func ServerUnaryInterceptor(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
	if UpdateMetrics != nil {
		UpdateMetrics(ctx)
	}
	if IsTampered {
		return nil, status.Errorf(
			codes.DataLoss, "the database should be checked manually as we detected possible tampering")
	}
	if !AuthEnabled {
		if !DevMode {
			if !isLocalClient(ctx) {
				return nil, status.Errorf(
					codes.PermissionDenied,
					"server has authentication disabled: only local connections are accepted")
			}
		}
	}
	return handler(ctx, req)
}

var localAddress = map[string]struct{}{
	"127.0.0.1": struct{}{},
	"localhost": struct{}{},
	"bufconn":   struct{}{},
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

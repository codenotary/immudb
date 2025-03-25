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

	"github.com/codenotary/immudb/pkg/api/schema"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// SignatureVerifierInterceptor verify that provided server signature match with the public key provided
func (c *immuClient) SignatureVerifierInterceptor(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
	ris := invoker(ctx, method, req, reply, cc, opts...)
	if c.serverSigningPubKey == nil {
		return status.Error(codes.FailedPrecondition, "public key not loaded")
	}
	if method == "/immudb.schema.ImmuService/CurrentState" {
		state := reply.(*schema.ImmutableState)
		err := state.CheckSignature(c.serverSigningPubKey)
		if err != nil {
			return status.Errorf(codes.InvalidArgument, "unable to verify signature: %s", err)
		}
	}
	return ris
}

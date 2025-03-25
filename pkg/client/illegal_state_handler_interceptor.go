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
	"errors"
	"strings"

	"google.golang.org/grpc"
)

// IllegalStateHandlerInterceptor improve UX on SDK adding more context when immudb returns an illegal state error message on Verifiable* methods
func (c *immuClient) IllegalStateHandlerInterceptor(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
	err := invoker(ctx, method, req, reply, cc, opts...)
	if err != nil && strings.Contains(method, "Verifiable") {
		if errors.Is(err, ErrSrvIllegalState) {
			serverState, err := c.CurrentState(ctx)
			if err != nil {
				return err
			}
			localState, err := c.StateService.GetState(ctx, serverState.Db)
			if err != nil {
				return err
			}
			if localState.TxId > serverState.TxId {
				return ErrServerStateIsOlder
			}
		}
	}
	return err
}

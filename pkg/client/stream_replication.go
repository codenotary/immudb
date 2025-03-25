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
)

// ExportTx retrieves serialized transaction object.
func (c *immuClient) ExportTx(ctx context.Context, req *schema.ExportTxRequest) (schema.ImmuService_ExportTxClient, error) {
	if req == nil {
		return nil, ErrIllegalArguments
	}

	if !c.IsConnected() {
		return nil, ErrNotConnected
	}

	return c.ServiceClient.ExportTx(ctx, req)
}

// ReplicateTx sends a previously serialized transaction object replicating it on another database.
func (c *immuClient) ReplicateTx(ctx context.Context) (schema.ImmuService_ReplicateTxClient, error) {
	if !c.IsConnected() {
		return nil, ErrNotConnected
	}

	return c.ServiceClient.ReplicateTx(ctx)
}

func (c *immuClient) StreamExportTx(ctx context.Context, opts ...grpc.CallOption) (schema.ImmuService_StreamExportTxClient, error) {
	if !c.IsConnected() {
		return nil, ErrNotConnected
	}

	return c.ServiceClient.StreamExportTx(ctx, opts...)
}

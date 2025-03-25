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
	"time"

	"github.com/codenotary/immudb/pkg/api/schema"
)

// TxOption is used to set additional options when creating a transaction with a NewTx call
type TxOption func(req *schema.NewTxRequest) error

// UnsafeMVCC option means the server is not forced to wait for the indexer
// to be up-to-date with the most recent transaction during conflict detection.
//
// In practice this means that the user will get the result faster with the
// risk of inconsistencies if another transaction invalidated the data read by this transaction.
//
// This option may be useful when it's guaranteed that related data is not concurrently
// written.
func UnsafeMVCC() TxOption {
	return func(req *schema.NewTxRequest) error {
		req.UnsafeMVCC = true
		return nil
	}
}

// An existing snapshot may be reused as long as it includes the specified transaction
func SnapshotMustIncludeTxID(txID uint64) TxOption {
	return func(req *schema.NewTxRequest) error {
		req.SnapshotMustIncludeTxID = &schema.NullableUint64{Value: txID}
		return nil
	}
}

// An existing snapshot may be reused as long as it is not older than the specified timeframe
func SnapshotRenewalPeriod(renewalPeriod time.Duration) TxOption {
	return func(req *schema.NewTxRequest) error {
		req.SnapshotRenewalPeriod = &schema.NullableMilliseconds{Value: renewalPeriod.Milliseconds()}
		return nil
	}
}

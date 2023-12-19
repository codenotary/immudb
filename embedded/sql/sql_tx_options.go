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

package sql

import (
	"fmt"
	"time"

	"github.com/codenotary/immudb/embedded/store"
)

type TxOptions struct {
	ReadOnly                bool
	SnapshotMustIncludeTxID func(lastPrecommittedTxID uint64) uint64
	SnapshotRenewalPeriod   time.Duration
	ExplicitClose           bool
	UnsafeMVCC              bool
	Extra                   []byte
}

func DefaultTxOptions() *TxOptions {
	txOpts := store.DefaultTxOptions()

	return &TxOptions{
		ReadOnly:                txOpts.Mode == store.ReadOnlyTx,
		SnapshotMustIncludeTxID: txOpts.SnapshotMustIncludeTxID,
		SnapshotRenewalPeriod:   txOpts.SnapshotRenewalPeriod,
		ExplicitClose:           false, // commit or rollback explicitly required
		UnsafeMVCC:              false, // mvcc restricted to catalog changes
	}
}

func (opts *TxOptions) Validate() error {
	if opts == nil {
		return fmt.Errorf("%w: nil options", store.ErrInvalidOptions)
	}

	return nil
}

func (opts *TxOptions) WithReadOnly(readOnly bool) *TxOptions {
	opts.ReadOnly = readOnly
	return opts
}

func (opts *TxOptions) WithSnapshotMustIncludeTxID(snapshotMustIncludeTxID func(lastPrecommittedTxID uint64) uint64) *TxOptions {
	opts.SnapshotMustIncludeTxID = snapshotMustIncludeTxID
	return opts
}

func (opts *TxOptions) WithSnapshotRenewalPeriod(snapshotRenewalPeriod time.Duration) *TxOptions {
	opts.SnapshotRenewalPeriod = snapshotRenewalPeriod
	return opts
}

func (opts *TxOptions) WithExplicitClose(explicitClose bool) *TxOptions {
	opts.ExplicitClose = explicitClose
	return opts
}

func (opts *TxOptions) WithUnsafeMVCC(unsafeMVCC bool) *TxOptions {
	opts.UnsafeMVCC = unsafeMVCC
	return opts
}

func (opts *TxOptions) WithExtra(data []byte) *TxOptions {
	opts.Extra = data
	return opts
}

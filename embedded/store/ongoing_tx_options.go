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

package store

import (
	"fmt"
	"time"
)

type TxMode int

const (
	ReadOnlyTx TxMode = iota
	WriteOnlyTx
	ReadWriteTx
)

type TxOptions struct {
	Mode TxMode
	// SnapshotMustIncludeTxID is a function which receives the latest precommitted transaction ID as parameter.
	// It gives the possibility to reuse a snapshot which includes a percentage of the transactions already indexed
	// e.g. func(lastPrecommittedTxID uint64) uint64 { return  lastPrecommittedTxID * 90 / 100 }
	// or just a fixed transaction ID e.g. func(_ uint64) uint64 { return  1_000 }
	SnapshotMustIncludeTxID func(lastPrecommittedTxID uint64) uint64
	// SnapshotRenewalPeriod determines for how long a snaphsot may reuse existent dumped root
	SnapshotRenewalPeriod time.Duration

	// MVCC does not wait for indexing to be up to date
	UnsafeMVCC bool
}

func DefaultTxOptions() *TxOptions {
	return &TxOptions{
		Mode: ReadWriteTx,
		SnapshotMustIncludeTxID: func(lastPrecommittedTxID uint64) uint64 {
			return lastPrecommittedTxID
		},
		UnsafeMVCC: false,
	}
}

func (opts *TxOptions) Validate() error {
	if opts == nil {
		return fmt.Errorf("%w: nil options", ErrInvalidOptions)
	}

	if opts.Mode != ReadOnlyTx && opts.Mode != WriteOnlyTx && opts.Mode != ReadWriteTx {
		return fmt.Errorf("%w: invalid transaction mode", ErrInvalidOptions)
	}

	return nil
}

func (opts *TxOptions) WithMode(mode TxMode) *TxOptions {
	opts.Mode = mode
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

func (opts *TxOptions) WithUnsafeMVCC(unsafeMVCC bool) *TxOptions {
	opts.UnsafeMVCC = unsafeMVCC
	return opts
}

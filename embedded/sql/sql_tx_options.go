/*
Copyright 2022 Codenotary Inc. All rights reserved.

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

package sql

import (
	"fmt"
	"time"

	"github.com/codenotary/immudb/embedded/store"
)

type TxMode store.TxMode

const (
	ReadOnlyTx  TxMode = TxMode(store.ReadOnlyTx)
	WriteOnlyTx TxMode = TxMode(store.WriteOnlyTx)
	ReadWriteTx TxMode = TxMode(store.ReadWriteTx)
)

type TxOptions struct {
	Mode                   TxMode
	SnapshotNotOlderThanTx func(lastPrecommittedTxID uint64) uint64
	SnapshotRenewalPeriod  time.Duration
}

func DefaultTxOptions() *TxOptions {
	txOpts := store.DefaultTxOptions()

	return &TxOptions{
		Mode:                   TxMode(txOpts.Mode),
		SnapshotNotOlderThanTx: txOpts.SnapshotNotOlderThanTx,
		SnapshotRenewalPeriod:  txOpts.SnapshotRenewalPeriod,
	}
}

func (opts *TxOptions) Validate() error {
	if opts == nil {
		return fmt.Errorf("%w: nil options", store.ErrInvalidOptions)
	}

	if opts.Mode != ReadOnlyTx && opts.Mode != WriteOnlyTx && opts.Mode != ReadWriteTx {
		return fmt.Errorf("%w: invalid transaction mode", store.ErrInvalidOptions)
	}

	return nil
}

func (opts *TxOptions) WithMode(mode TxMode) *TxOptions {
	opts.Mode = mode
	return opts
}

func (opts *TxOptions) WithSnapshotNotOlderThanTx(snapshotNotOlderThanTx func(lastPrecommittedTxID uint64) uint64) *TxOptions {
	opts.SnapshotNotOlderThanTx = snapshotNotOlderThanTx
	return opts
}

func (opts *TxOptions) WithSnapshotRenewalPeriod(snapshotRenewalPeriod time.Duration) *TxOptions {
	opts.SnapshotRenewalPeriod = snapshotRenewalPeriod
	return opts
}

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
	Mode                    TxMode
	SnapshotMustIncludeTxID func(lastPrecommittedTxID uint64) uint64
	SnapshotRenewalPeriod   time.Duration
}

func DefaultTxOptions() *TxOptions {
	return &TxOptions{
		Mode: ReadWriteTx,
		SnapshotMustIncludeTxID: func(lastPrecommittedTxID uint64) uint64 {
			return lastPrecommittedTxID
		},
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

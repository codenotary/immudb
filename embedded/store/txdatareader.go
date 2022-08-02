/*
Copyright 2022 CodeNotary, Inc. All rights reserved.

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
	"github.com/codenotary/immudb/embedded/appendable"
	"github.com/codenotary/immudb/embedded/htree"
)

type TxDataReader interface {

	// AllocEntry allocates temporary placeholder for an TxEntry value.
	// Can be used as long as the TxDataReader is not closed
	AllocTxEntry() *TxEntry

	// NEntries returns the number of entries
	NEntries() int

	// GetIncompleteHeader returns the header that may not have the EH field calculated yet
	GetIncompleteHeader() *TxHeader

	// ReadEntry reads next entry from the transaction
	ReadEntry(e *TxEntry) error

	// GetHeader returns the transaction header
	//
	// Note: this call will only succeed once all entries of a transaction are read since
	// some fields of the header can only be calculated after reading all entries.
	// Calling this method before all entries are read will end up with a nil value.
	GetHeader() *TxHeader

	// GetHtree returns the internal transaction merkle tree

	// Note: this call will only succeed once all entries of a transaction are read since
	// some the internal hash tree can only be calculated after reading all entries.
	// Calling this method before all entries are read will end up with a nil value.
	GetHTree() *htree.HTree

	// Close closes the instance freeing up all buffers etc
	Close() error
}

type txDataReaderInst struct {
	tdr         txDataReader
	maxKeyLen   int
	hdr         *TxHeader
	htree       *htree.HTree
	entriesLeft int
}

func newTxDataReaderInst(r *appendable.Reader, maxEntries, maxKeyLen int) (TxDataReader, error) {
	ret := &txDataReaderInst{
		tdr:       txDataReader{r: r},
		maxKeyLen: maxKeyLen,
	}

	hdr, err := ret.tdr.readHeader(maxEntries)
	if err != nil {
		return nil, err
	}
	ret.hdr = hdr

	ret.entriesLeft = ret.hdr.NEntries

	return ret, nil
}

func (t *txDataReaderInst) AllocTxEntry() *TxEntry {
	return &TxEntry{k: make([]byte, t.maxKeyLen)}
}

func (t *txDataReaderInst) NEntries() int {
	return t.hdr.NEntries
}

func (t *txDataReaderInst) GetIncompleteHeader() *TxHeader {
	return t.hdr
}

func (t *txDataReaderInst) ReadEntry(e *TxEntry) error {
	if t.entriesLeft <= 0 {
		return ErrNoMoreEntries
	}
	t.entriesLeft--

	err := t.tdr.readEntry(e)
	if err != nil {
		return err
	}

	if t.entriesLeft == 0 {
		t.htree, err = htree.New(t.hdr.NEntries)
		if err != nil {
			return err
		}

		err = t.tdr.buildAndValidateHtree(t.htree)
		if err != nil {
			return err
		}
	}

	return nil
}

func (t *txDataReaderInst) GetHeader() *TxHeader {
	if t.entriesLeft > 0 {
		return nil
	}

	return t.hdr
}

func (t *txDataReaderInst) GetHTree() *htree.HTree {
	if t.entriesLeft > 0 {
		return nil
	}

	return t.htree
}

func (t *txDataReaderInst) Close() error {
	return nil
}

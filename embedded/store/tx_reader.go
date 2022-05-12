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
	"crypto/sha256"
	"fmt"
)

type TxReader struct {
	InitialTxID uint64
	Desc        bool

	CurrTxID uint64
	CurrAlh  [sha256.Size]byte

	st  *ImmuStore
	_tx *Tx
}

func (s *ImmuStore) NewTxReader(initialTxID uint64, desc bool, tx *Tx) (*TxReader, error) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	if s.closed {
		return nil, ErrAlreadyClosed
	}

	return s.newTxReader(initialTxID, desc, tx)
}

func (s *ImmuStore) newTxReader(initialTxID uint64, desc bool, tx *Tx) (*TxReader, error) {
	if initialTxID == 0 {
		return nil, ErrIllegalArguments
	}

	if tx == nil {
		return nil, ErrIllegalArguments
	}

	return &TxReader{
		InitialTxID: initialTxID,
		Desc:        desc,
		CurrTxID:    initialTxID,
		st:          s,
		_tx:         tx,
	}, nil
}

func (txr *TxReader) Read() (*Tx, error) {
	if txr.CurrTxID == 0 {
		return nil, ErrNoMoreEntries
	}

	err := txr.st.ReadTx(txr.CurrTxID, txr._tx)
	if err == ErrTxNotFound {
		return nil, ErrNoMoreEntries
	}
	if err != nil {
		return nil, txr.st.wrapAppendableErr(err, "reading transaction")
	}

	if txr.InitialTxID != txr.CurrTxID {
		if txr.Desc && txr.CurrAlh != txr._tx.header.Alh() {
			return nil, fmt.Errorf("%w: ALH mismatch at tx %d", ErrorCorruptedTxData, txr._tx.header.ID)
		}

		if !txr.Desc && txr.CurrAlh != txr._tx.header.PrevAlh {
			return nil, fmt.Errorf("%w: ALH mismatch at tx %d", ErrorCorruptedTxData, txr._tx.header.ID)
		}
	}

	if txr.Desc {
		txr.CurrTxID--
		txr.CurrAlh = txr._tx.header.PrevAlh
	} else {
		txr.CurrTxID++
		txr.CurrAlh = txr._tx.header.Alh()
	}

	return txr._tx, nil
}

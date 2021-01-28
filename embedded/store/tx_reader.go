/*
Copyright 2019-2020 vChain, Inc.

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
	"io"
	"sync"

	"github.com/codenotary/immudb/embedded/appendable"
)

type TxReader struct {
	r           *appendable.Reader
	_tx         *Tx
	alreadyRead bool
	txID        uint64
	alh         [sha256.Size]byte
}

func (s *ImmuStore) NewTxReader(txID uint64, tx *Tx, bufSize int) (*TxReader, error) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	if s.closed {
		return nil, ErrAlreadyClosed
	}

	if tx == nil {
		return nil, ErrIllegalArguments
	}

	syncedReader := &syncedReader{wr: s.txLog, maxSize: s.committedTxLogSize, mutex: &s.mutex}

	txOff, _, err := s.txOffsetAndSize(txID)
	if err == io.EOF {
		return nil, ErrNoMoreEntries
	}
	if err != nil {
		return nil, err
	}

	r := appendable.NewReaderFrom(syncedReader, txOff, bufSize)

	return &TxReader{r: r, _tx: tx}, nil
}

func (txr *TxReader) Read() (*Tx, error) {
	err := txr._tx.readFrom(txr.r)
	if err == io.EOF {
		return nil, ErrNoMoreEntries
	}
	if err != nil {
		return nil, err
	}

	if txr.alreadyRead && (txr.txID != txr._tx.ID-1 || txr.alh != txr._tx.PrevAlh) {
		return nil, ErrorCorruptedTxData
	}

	txr.alreadyRead = true
	txr.txID = txr._tx.ID
	txr.alh = txr._tx.Alh

	return txr._tx, nil
}

type syncedReader struct {
	wr      io.ReaderAt
	maxSize int64
	mutex   *sync.Mutex
}

func (r *syncedReader) ReadAt(bs []byte, off int64) (int, error) {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	if len(bs) == 0 {
		return 0, nil
	}

	available := minInt(len(bs), int(r.maxSize-off))

	if r.maxSize < off || available == 0 {
		return 0, io.EOF
	}

	return r.wr.ReadAt(bs[:available], off)
}

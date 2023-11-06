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
	"context"
	"errors"
)

type expectedReader struct {
	spec          KeyReaderSpec
	expectedReads [][]expectedRead // multiple []expectedRead may be generated if the reader is reset
	i             int              // it matches with reset count, used to point to the latest []expectedRead
}

type expectedRead struct {
	initialTxID uint64
	finalTxID   uint64

	expectedKey []byte
	expectedTx  uint64 // expectedTx = 0 means the entry was updated/created by the ongoing transaction

	expectedNoMoreEntries bool
}

// ongoingTxKeyReader wraps a keyReader and keeps track of read entries
// read entries are validated against the current database state at commit time
type ongoingTxKeyReader struct {
	tx *OngoingTx

	keyReader KeyReader
	offset    uint64 // offset and filtering is handled by the wrapper in order to have full control of read entries
	skipped   uint64

	expectedReader *expectedReader
}

func newExpectedReader(spec KeyReaderSpec) *expectedReader {
	return &expectedReader{
		spec:          spec,
		expectedReads: make([][]expectedRead, 1),
	}
}

func newOngoingTxKeyReader(tx *OngoingTx, spec KeyReaderSpec) (*ongoingTxKeyReader, error) {
	if tx.mvccReadSetLimitReached() {
		return nil, ErrMVCCReadSetLimitExceeded
	}

	snap, err := tx.snap(spec.Prefix)
	if err != nil {
		return nil, err
	}

	rspec := KeyReaderSpec{
		SeekKey:       spec.SeekKey,
		EndKey:        spec.EndKey,
		Prefix:        spec.Prefix,
		InclusiveSeek: spec.InclusiveSeek,
		InclusiveEnd:  spec.InclusiveEnd,
		DescOrder:     spec.DescOrder,
	}

	keyReader, err := snap.NewKeyReader(rspec)
	if err != nil {
		return nil, err
	}

	expectedReader := newExpectedReader(spec)

	tx.mvccReadSet.expectedReaders = append(tx.mvccReadSet.expectedReaders, expectedReader)
	tx.mvccReadSet.readsetSize++

	return &ongoingTxKeyReader{
		tx:             tx,
		keyReader:      keyReader,
		offset:         spec.Offset,
		expectedReader: expectedReader,
	}, nil
}

func (r *ongoingTxKeyReader) Read(ctx context.Context) (key []byte, val ValueRef, err error) {
	return r.ReadBetween(ctx, 0, 0)
}

func (r *ongoingTxKeyReader) ReadBetween(ctx context.Context, initialTxID, finalTxID uint64) (key []byte, valRef ValueRef, err error) {
	for {
		if initialTxID == 0 && finalTxID == 0 {
			key, valRef, err = r.keyReader.Read(ctx)
		} else {
			key, valRef, err = r.keyReader.ReadBetween(ctx, initialTxID, finalTxID)
		}

		if errors.Is(err, ErrNoMoreEntries) {
			expectedRead := expectedRead{
				initialTxID:           initialTxID,
				finalTxID:             finalTxID,
				expectedNoMoreEntries: true,
			}

			if r.tx.mvccReadSet.readsetSize == r.tx.st.mvccReadSetLimit {
				return nil, nil, ErrMVCCReadSetLimitExceeded
			}

			r.expectedReader.expectedReads[r.expectedReader.i] = append(r.expectedReader.expectedReads[r.expectedReader.i], expectedRead)
			r.tx.mvccReadSet.readsetSize++
		}

		if err != nil {
			return nil, nil, err
		}

		expectedRead := expectedRead{
			initialTxID: initialTxID,
			finalTxID:   finalTxID,
			expectedKey: cp(key),
			expectedTx:  valRef.Tx(),
		}

		if r.tx.mvccReadSet.readsetSize == r.tx.st.mvccReadSetLimit {
			return nil, nil, ErrMVCCReadSetLimitExceeded
		}

		r.expectedReader.expectedReads[r.expectedReader.i] = append(r.expectedReader.expectedReads[r.expectedReader.i], expectedRead)
		r.tx.mvccReadSet.readsetSize++

		filterEntry := false

		for _, filter := range r.expectedReader.spec.Filters {
			err = filter(valRef, r.tx.Timestamp())
			if err != nil {
				filterEntry = true
				break
			}
		}

		if filterEntry {
			continue
		}

		if r.skipped < r.offset {
			r.skipped++
			continue
		}

		return key, valRef, nil
	}
}

func (r *ongoingTxKeyReader) Reset() error {
	err := r.keyReader.Reset()
	if err != nil {
		return err
	}

	if r.tx.mvccReadSet.readsetSize == r.tx.st.mvccReadSetLimit {
		return ErrMVCCReadSetLimitExceeded
	}

	r.expectedReader.expectedReads = append(r.expectedReader.expectedReads, nil)
	r.expectedReader.i++

	r.tx.mvccReadSet.readsetSize++

	return nil
}

func (r *ongoingTxKeyReader) Close() error {
	return r.keyReader.Close()
}

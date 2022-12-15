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

import "errors"

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

type ongoingTxKeyReader struct {
	tx *OngoingTx

	keyReader KeyReader
	offset    uint64
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
	rspec := KeyReaderSpec{
		SeekKey:       spec.SeekKey,
		EndKey:        spec.EndKey,
		Prefix:        spec.Prefix,
		InclusiveSeek: spec.InclusiveSeek,
		InclusiveEnd:  spec.InclusiveEnd,
		DescOrder:     spec.DescOrder,
	}

	keyReader, err := tx.snap.NewKeyReader(rspec)
	if err != nil {
		return nil, err
	}

	expectedReader := newExpectedReader(spec)

	tx.expectedReaders = append(tx.expectedReaders, expectedReader)

	return &ongoingTxKeyReader{
		tx:             tx,
		keyReader:      keyReader,
		offset:         spec.Offset,
		expectedReader: expectedReader,
	}, nil
}

func (r *ongoingTxKeyReader) Read() (key []byte, val ValueRef, err error) {
	return r.ReadBetween(0, 0)
}

func (r *ongoingTxKeyReader) ReadBetween(initialTxID, finalTxID uint64) (key []byte, valRef ValueRef, err error) {
	for {
		if initialTxID == 0 && finalTxID == 0 {
			key, valRef, err = r.keyReader.Read()
		} else {
			key, valRef, err = r.keyReader.ReadBetween(initialTxID, finalTxID)
		}

		if errors.Is(err, ErrNoMoreEntries) {
			expectedRead := expectedRead{
				initialTxID:           initialTxID,
				finalTxID:             finalTxID,
				expectedNoMoreEntries: true,
			}

			r.expectedReader.expectedReads[r.expectedReader.i] = append(r.expectedReader.expectedReads[r.expectedReader.i], expectedRead)
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

		r.expectedReader.expectedReads[r.expectedReader.i] = append(r.expectedReader.expectedReads[r.expectedReader.i], expectedRead)

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

	r.expectedReader.expectedReads = append(r.expectedReader.expectedReads, nil)
	r.expectedReader.i++

	return nil
}

func (r *ongoingTxKeyReader) Close() error {
	return r.keyReader.Close()
}

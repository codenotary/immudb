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

type expectedReader struct {
	spec          KeyReaderSpec
	expectedReads [][]expectedRead
}

type expectedRead struct {
	initialTxID uint64
	finalTxID   uint64

	expectedKey []byte
	expectedTx  uint64
}

type ongoingTxKeyReader struct {
	keyReader      KeyReader
	expectedReader *expectedReader
}

func newExpectedReader(spec KeyReaderSpec) *expectedReader {
	return &expectedReader{
		spec:          spec,
		expectedReads: make([][]expectedRead, 1),
	}
}

func newOngoingTxKeyReader(keyReader KeyReader, expectedReader *expectedReader) *ongoingTxKeyReader {
	return &ongoingTxKeyReader{
		keyReader:      keyReader,
		expectedReader: expectedReader,
	}
}

func (r *ongoingTxKeyReader) Read() (key []byte, val ValueRef, err error) {
	key, valRef, err := r.keyReader.Read()
	if err != nil {
		return nil, nil, err
	}

	if valRef.Tx() > 0 {
		// it only requires validation when the entry was pre-existent to ongoing tx
		expectedRead := expectedRead{
			expectedKey: cp(key),
			expectedTx:  valRef.Tx(),
		}

		i := len(r.expectedReader.expectedReads) - 1

		r.expectedReader.expectedReads[i] = append(r.expectedReader.expectedReads[i], expectedRead)
	}

	return key, valRef, nil
}

func (r *ongoingTxKeyReader) ReadBetween(initialTxID, finalTxID uint64) (key []byte, val ValueRef, err error) {
	key, valRef, err := r.keyReader.ReadBetween(initialTxID, finalTxID)
	if err != nil {
		return nil, nil, err
	}

	if valRef.Tx() > 0 {
		// it only requires validation when the entry was pre-existent to ongoing tx

		expectedRead := expectedRead{
			initialTxID: initialTxID,
			finalTxID:   finalTxID,
			expectedKey: cp(key),
			expectedTx:  valRef.Tx(),
		}

		i := len(r.expectedReader.expectedReads) - 1

		r.expectedReader.expectedReads[i] = append(r.expectedReader.expectedReads[i], expectedRead)
	}

	return key, valRef, nil
}

func (r *ongoingTxKeyReader) Reset() error {
	err := r.keyReader.Reset()
	if err != nil {
		return err
	}

	r.expectedReader.expectedReads = append(r.expectedReader.expectedReads, nil)

	return nil
}

func (r *ongoingTxKeyReader) Close() error {
	return r.keyReader.Close()
}

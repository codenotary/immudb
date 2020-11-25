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
	"errors"
	"testing"

	"codenotary.io/immudb-v2/appendable"
	"codenotary.io/immudb-v2/appendable/mocked"
	"github.com/stretchr/testify/require"
)

func TestReadTxFromCorruptedData(t *testing.T) {

	a := &mocked.MockedAppendable{}

	r := appendable.NewReaderFrom(a, 0, 1)
	require.NotNil(t, r)

	tx := newTx(1, 32)

	// Should fail while reading TxID
	a.ReadAtFn = func(bs []byte, off int64) (int, error) {
		return 0, errors.New("error")
	}
	err := tx.readFrom(r)
	require.Error(t, err)

	// Should fail while reading Ts
	r.Reset()
	a.ReadAtFn = func(bs []byte, off int64) (int, error) {
		if off < 8 {
			return len(bs), nil
		}
		return 0, errors.New("error")
	}
	err = tx.readFrom(r)
	require.Error(t, err)

	// Should fail while reading BlTxID
	r.Reset()
	a.ReadAtFn = func(bs []byte, off int64) (int, error) {
		if off < 16 {
			return len(bs), nil
		}
		return 0, errors.New("error")
	}
	err = tx.readFrom(r)
	require.Error(t, err)

	// Should fail while reading BlRoot
	r.Reset()
	a.ReadAtFn = func(bs []byte, off int64) (int, error) {
		if off < 24+sha256.Size {
			return len(bs), nil
		}
		return 0, errors.New("error")
	}
	err = tx.readFrom(r)
	require.Error(t, err)

	// Should fail while reading PrevAlh
	r.Reset()
	a.ReadAtFn = func(bs []byte, off int64) (int, error) {
		if off < 24+2*sha256.Size {
			return len(bs), nil
		}
		return 0, errors.New("error")
	}
	err = tx.readFrom(r)
	require.Error(t, err)

	// Should fail while reading nentries
	r.Reset()
	a.ReadAtFn = func(bs []byte, off int64) (int, error) {
		if off < 32+2*sha256.Size {
			return len(bs), nil
		}
		return 0, errors.New("error")
	}
	err = tx.readFrom(r)
	require.Error(t, err)
}

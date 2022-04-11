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
	"testing"

	"github.com/stretchr/testify/require"
)

func TestOngoingTXAddPrecondition(t *testing.T) {
	otx := OngoingTx{
		st: &ImmuStore{
			maxKeyLen: 10,
		},
	}

	err := otx.AddPrecondition(nil)
	require.ErrorIs(t, err, ErrIllegalArguments)

	err = otx.AddPrecondition(&PreconditionKeyMustExist{})
	require.ErrorIs(t, err, ErrInvalidPrecondition)

	otx.closed = true
	err = otx.AddPrecondition(&PreconditionKeyMustExist{
		Key: []byte("key"),
	})
	require.ErrorIs(t, err, ErrAlreadyClosed)
}

func TestOngoingTxCheckPreconditionsCornerCases(t *testing.T) {
	otx := &OngoingTx{}
	idx := &dummyKeyIndex{}

	err := otx.checkPreconditions(idx)
	require.NoError(t, err)

	otx.preconditions = []Precondition{nil}
	err = otx.checkPreconditions(idx)
	require.ErrorIs(t, err, ErrInvalidPrecondition)
	require.ErrorIs(t, err, ErrInvalidPreconditionNull)

	idx.closed = true
	otx.preconditions = []Precondition{
		&PreconditionKeyMustExist{Key: []byte{1}},
	}
	err = otx.checkPreconditions(idx)
	require.ErrorIs(t, err, ErrAlreadyClosed)

	otx.preconditions = []Precondition{
		&PreconditionKeyMustNotExist{Key: []byte{1}},
	}
	err = otx.checkPreconditions(idx)
	require.ErrorIs(t, err, ErrAlreadyClosed)

	otx.preconditions = []Precondition{
		&PreconditionKeyNotModifiedAfterTx{Key: []byte{1}, TxID: 1},
	}
	err = otx.checkPreconditions(idx)
	require.ErrorIs(t, err, ErrAlreadyClosed)
}

type dummyKeyIndex struct {
	closed bool
}

func (i *dummyKeyIndex) Get(key []byte) (valRef ValueRef, err error) {
	return i.GetWith(key, IgnoreDeleted)
}

func (i *dummyKeyIndex) GetWith(key []byte, filters ...FilterFn) (valRef ValueRef, err error) {
	if i.closed {
		return nil, ErrAlreadyClosed
	}

	return nil, ErrKeyNotFound
}

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

func TestOngoingTXAddKVConstraint(t *testing.T) {
	otx := OngoingTx{
		st: &ImmuStore{
			maxKeyLen: 10,
		},
	}

	err := otx.AddKVConstraint(nil)
	require.ErrorIs(t, err, ErrIllegalArguments)

	err = otx.AddKVConstraint(&WriteContraintKeyMustExist{})
	require.ErrorIs(t, err, ErrInvalidConstraints)

	otx.closed = true
	err = otx.AddKVConstraint(&WriteContraintKeyMustExist{
		Key: []byte("key"),
	})
	require.ErrorIs(t, err, ErrAlreadyClosed)
}

func TestOngoingTxCheckWriteConstraintsCornerCases(t *testing.T) {
	otx := &OngoingTx{}
	idx := &indexer{}

	err := otx.checkWriteConstraints(idx)
	require.NoError(t, err)

	otx.constraints = []WriteConstraint{nil}
	err = otx.checkWriteConstraints(idx)
	require.ErrorIs(t, err, ErrInvalidConstraints)
	require.ErrorIs(t, err, ErrInvalidConstraintsNull)

	idx.closed = true
	otx.constraints = []WriteConstraint{
		&WriteContraintKeyMustExist{Key: []byte{1}},
	}
	err = otx.checkWriteConstraints(idx)
	require.ErrorIs(t, err, ErrAlreadyClosed)

	otx.constraints = []WriteConstraint{
		&WriteContraintKeyMustNotExist{Key: []byte{1}},
	}
	err = otx.checkWriteConstraints(idx)
	require.ErrorIs(t, err, ErrAlreadyClosed)

	otx.constraints = []WriteConstraint{
		&WriteContraintKeyNotModifiedAfterTx{Key: []byte{1}, TxID: 1},
	}
	err = otx.checkWriteConstraints(idx)
	require.ErrorIs(t, err, ErrAlreadyClosed)
}

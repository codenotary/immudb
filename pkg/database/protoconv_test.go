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
package database

import (
	"testing"

	"github.com/codenotary/immudb/embedded/store"
	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/stretchr/testify/require"
)

type fakeWriteConstraint struct{}

func (*fakeWriteConstraint) isWriteConstraint_Constraint() {}

func TestWriteConstraintsFromProto(t *testing.T) {

	t.Run("Nil constraints", func(t *testing.T) {
		_, err := WriteConstraintsFromProto(nil)
		require.ErrorIs(t, err, store.ErrInvalidConstraints)
		require.ErrorIs(t, err, store.ErrInvalidConstraintsNull)

		_, err = WriteConstraintsFromProto(&schema.WriteConstraint{})
		require.ErrorIs(t, err, store.ErrInvalidConstraints)
		require.ErrorIs(t, err, store.ErrInvalidConstraintsNull)
	})

	t.Run("KeyMustExist", func(t *testing.T) {
		_, err := WriteConstraintsFromProto(schema.WriteConstraintKeyMustExist(nil))
		require.ErrorIs(t, err, store.ErrInvalidConstraints)
		require.ErrorIs(t, err, store.ErrInvalidConstraintsNullKey)

		_, err = WriteConstraintsFromProto(schema.WriteConstraintKeyMustExist([]byte{}))
		require.ErrorIs(t, err, store.ErrInvalidConstraints)
		require.ErrorIs(t, err, store.ErrInvalidConstraintsNullKey)

		c, err := WriteConstraintsFromProto(schema.WriteConstraintKeyMustExist([]byte{1}))
		require.NoError(t, err)
		require.IsType(t, &store.WriteContraintKeyMustExist{}, c)
	})

	t.Run("KeyMustNotExist", func(t *testing.T) {
		_, err := WriteConstraintsFromProto(schema.WriteConstraintKeyMustNotExist(nil))
		require.ErrorIs(t, err, store.ErrInvalidConstraints)
		require.ErrorIs(t, err, store.ErrInvalidConstraintsNullKey)

		_, err = WriteConstraintsFromProto(schema.WriteConstraintKeyMustNotExist([]byte{}))
		require.ErrorIs(t, err, store.ErrInvalidConstraints)
		require.ErrorIs(t, err, store.ErrInvalidConstraintsNullKey)

		c, err := WriteConstraintsFromProto(schema.WriteConstraintKeyMustNotExist([]byte{1}))
		require.NoError(t, err)
		require.IsType(t, &store.WriteContraintKeyMustNotExist{}, c)
	})

	t.Run("KeyNotModifiedAfterTX", func(t *testing.T) {
		_, err := WriteConstraintsFromProto(schema.WriteConstraintKeyNotModifiedAfterTX(nil, 0))
		require.ErrorIs(t, err, store.ErrInvalidConstraints)
		require.ErrorIs(t, err, store.ErrInvalidConstraintsNullKey)

		_, err = WriteConstraintsFromProto(schema.WriteConstraintKeyNotModifiedAfterTX([]byte{}, 0))
		require.ErrorIs(t, err, store.ErrInvalidConstraints)
		require.ErrorIs(t, err, store.ErrInvalidConstraintsNullKey)

		c, err := WriteConstraintsFromProto(schema.WriteConstraintKeyNotModifiedAfterTX([]byte{1}, 1))
		require.NoError(t, err)
		require.IsType(t, &store.WriteContraintKeyNotModifiedAfterTx{}, c)
	})

}

/*
Copyright 2025 Codenotary Inc. All rights reserved.

SPDX-License-Identifier: BUSL-1.1
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    https://mariadb.com/bsl11/

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

func TestPreconditionFromProto(t *testing.T) {

	t.Run("Nil precondition", func(t *testing.T) {
		_, err := PreconditionFromProto(nil)
		require.ErrorIs(t, err, store.ErrInvalidPrecondition)
		require.ErrorIs(t, err, store.ErrInvalidPreconditionNull)

		_, err = PreconditionFromProto(&schema.Precondition{})
		require.ErrorIs(t, err, store.ErrInvalidPrecondition)
		require.ErrorIs(t, err, store.ErrInvalidPreconditionNull)
	})

	t.Run("KeyMustExist", func(t *testing.T) {
		_, err := PreconditionFromProto(schema.PreconditionKeyMustExist(nil))
		require.ErrorIs(t, err, store.ErrInvalidPrecondition)
		require.ErrorIs(t, err, store.ErrInvalidPreconditionNullKey)

		_, err = PreconditionFromProto(schema.PreconditionKeyMustExist([]byte{}))
		require.ErrorIs(t, err, store.ErrInvalidPrecondition)
		require.ErrorIs(t, err, store.ErrInvalidPreconditionNullKey)

		c, err := PreconditionFromProto(schema.PreconditionKeyMustExist([]byte{1}))
		require.NoError(t, err)
		require.IsType(t, &store.PreconditionKeyMustExist{}, c)
	})

	t.Run("KeyMustNotExist", func(t *testing.T) {
		_, err := PreconditionFromProto(schema.PreconditionKeyMustNotExist(nil))
		require.ErrorIs(t, err, store.ErrInvalidPrecondition)
		require.ErrorIs(t, err, store.ErrInvalidPreconditionNullKey)

		_, err = PreconditionFromProto(schema.PreconditionKeyMustNotExist([]byte{}))
		require.ErrorIs(t, err, store.ErrInvalidPrecondition)
		require.ErrorIs(t, err, store.ErrInvalidPreconditionNullKey)

		c, err := PreconditionFromProto(schema.PreconditionKeyMustNotExist([]byte{1}))
		require.NoError(t, err)
		require.IsType(t, &store.PreconditionKeyMustNotExist{}, c)
	})

	t.Run("KeyNotModifiedAfterTX", func(t *testing.T) {
		_, err := PreconditionFromProto(schema.PreconditionKeyNotModifiedAfterTX(nil, 0))
		require.ErrorIs(t, err, store.ErrInvalidPrecondition)
		require.ErrorIs(t, err, store.ErrInvalidPreconditionNullKey)

		_, err = PreconditionFromProto(schema.PreconditionKeyNotModifiedAfterTX([]byte{}, 0))
		require.ErrorIs(t, err, store.ErrInvalidPrecondition)
		require.ErrorIs(t, err, store.ErrInvalidPreconditionNullKey)

		c, err := PreconditionFromProto(schema.PreconditionKeyNotModifiedAfterTX([]byte{1}, 1))
		require.NoError(t, err)
		require.IsType(t, &store.PreconditionKeyNotModifiedAfterTx{}, c)
	})

}

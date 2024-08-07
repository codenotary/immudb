/*
Copyright 2024 Codenotary Inc. All rights reserved.

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

package server

import (
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"testing"

	"github.com/codenotary/immudb/embedded/sql"
	"github.com/stretchr/testify/require"
)

func Test_getInt64(t *testing.T) {
	b64i := make([]byte, 8)
	binary.BigEndian.PutUint64(b64i, 1)
	i, err := getInt64(b64i)
	require.NoError(t, err)
	require.Equal(t, int64(1), i)
	b32i := make([]byte, 4)
	binary.BigEndian.PutUint32(b32i, 1)
	i, err = getInt64(b32i)
	require.NoError(t, err)
	require.Equal(t, int64(1), i)
	b16i := make([]byte, 2)
	binary.BigEndian.PutUint16(b16i, 1)
	i, err = getInt64(b16i)
	require.NoError(t, err)
	require.Equal(t, int64(1), i)

	bxxx := make([]byte, 64)
	_, err = getInt64(bxxx)
	require.ErrorContains(t, err, fmt.Sprintf("cannot convert a slice of %d byte in an INTEGER parameter", len(bxxx)))
}

func Test_buildNamedParams(t *testing.T) {
	// integer error
	cols := []sql.ColDescriptor{
		{
			Column: "p1",
			Type:   "INTEGER",
		},
	}
	pt := []interface{}{[]byte(`1`)}
	_, err := buildNamedParams(cols, pt)
	require.ErrorContains(t, err, fmt.Sprintf("cannot convert a slice of %d byte in an INTEGER parameter", len(cols)))

	// varchar error
	cols = []sql.ColDescriptor{
		{
			Column: "p1",
			Type:   "VARCHAR",
		},
	}
	pt = []interface{}{[]byte(`1`)}
	_, err = buildNamedParams(cols, pt)
	require.NoError(t, err)

	// blob
	cols = []sql.ColDescriptor{
		{
			Column: "p1",
			Type:   "BLOB",
		},
	}
	pt = []interface{}{[]byte(`1`)}
	_, err = buildNamedParams(cols, pt)
	require.NoError(t, err)

	// blob text error
	cols = []sql.ColDescriptor{
		{
			Column: "p1",
			Type:   "BLOB",
		},
	}
	pt = []interface{}{"blob"}
	_, err = buildNamedParams(cols, pt)
	require.ErrorIs(t, err, hex.InvalidByteError(108))
}

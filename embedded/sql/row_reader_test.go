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

package sql

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestKeyReaderSpecFromCornerCases(t *testing.T) {
	prefix := []byte("key.prefix.")
	table := &Table{
		id: 2,
	}
	index := &Index{
		table: table,
		id:    3,
		cols: []*Column{
			{
				id:     4,
				maxLen: 0,
			},
		},
	}

	t.Run("fail on invalid hrange", func(t *testing.T) {
		scanSpecs := &ScanSpecs{
			Index: index,
			rangesByColID: map[uint32]*typedValueRange{
				4: {
					hRange: &typedValueSemiRange{
						val: &Varchar{val: "test"},
					},
				},
			},
		}

		_, err := keyReaderSpecFrom(prefix, table, scanSpecs)
		require.ErrorIs(t, err, ErrInvalidValue)
	})

	t.Run("fail on invalid lrange", func(t *testing.T) {
		scanSpecs := &ScanSpecs{
			Index: index,
			rangesByColID: map[uint32]*typedValueRange{
				4: {
					lRange: &typedValueSemiRange{
						val: &Varchar{val: "test"},
					},
				},
			},
		}

		_, err := keyReaderSpecFrom(prefix, table, scanSpecs)
		require.ErrorIs(t, err, ErrInvalidValue)
	})
}

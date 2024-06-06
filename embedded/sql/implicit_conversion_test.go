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
	"fmt"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestApplyImplicitConversion(t *testing.T) {
	for _, d := range []struct {
		val          interface{}
		requiredType SQLValueType
		expected     interface{}
	}{
		{1, IntegerType, int64(1)},
		{1, Float64Type, float64(1)},
		{1.0, Float64Type, float64(1)},
		{"1", IntegerType, int64(1)},
		{"4.2", Float64Type, float64(4.2)},
		// UUID Version 4, RFC4122 Variant
		{"00010203-0440-0680-0809-0a0b0c0d0e0f", UUIDType, uuid.UUID([16]byte{0x00, 0x01, 0x02, 0x03, 0x04, 0x40, 0x06, 0x80, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f})},
		{[]byte{0x00, 0x01, 0x02, 0x03, 0x04, 0x40, 0x06, 0x80, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f}, UUIDType, uuid.UUID([16]byte{0x00, 0x01, 0x02, 0x03, 0x04, 0x40, 0x06, 0x80, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f})},
	} {
		t.Run(fmt.Sprintf("%+v", d), func(t *testing.T) {
			convVal, err := mayApplyImplicitConversion(d.val, d.requiredType)
			require.NoError(t, err)
			require.EqualValues(t, d.expected, convVal)
		})
	}
}

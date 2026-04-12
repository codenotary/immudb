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

package bmessages

import (
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestEmptyQueryResponse(t *testing.T) {
	resp := EmptyQueryResponse()
	require.NotNil(t, resp)

	// Message type 'I'
	require.Equal(t, byte('I'), resp[0])

	// Message length (4 bytes, value = 4)
	length := binary.BigEndian.Uint32(resp[1:5])
	require.Equal(t, uint32(4), length)

	require.Len(t, resp, 5)
}

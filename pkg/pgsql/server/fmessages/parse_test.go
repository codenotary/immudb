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

package fmessages

import (
	"fmt"
	"io"
	"testing"

	h "github.com/codenotary/immudb/pkg/pgsql/server/fmessages/fmessages_test"
	"github.com/stretchr/testify/require"
)

func TestParsedMsg(t *testing.T) {
	var tests = []struct {
		in  []byte
		out ParseMsg
		e   error
	}{
		{h.Join([][]byte{h.S("st"), h.S("statement"), h.I16(1), h.I32(1)}),
			ParseMsg{
				ParamsCount:               1,
				DestPreparedStatementName: "st",
				Statements:                "statement",
				ObjectIDs:                 []int32{1},
			},
			nil,
		},
		{h.Join([][]byte{h.S("st"), h.S("statement"), h.I16(1)}),
			ParseMsg{},
			io.EOF,
		},
		{h.Join([][]byte{h.S("st"), h.S("statement")}),
			ParseMsg{},
			io.EOF,
		},
		{h.Join([][]byte{h.S("st")}),
			ParseMsg{},
			io.EOF,
		},
		{h.Join([][]byte{}),
			ParseMsg{},
			io.EOF,
		},
	}

	for i, tt := range tests {
		t.Run(fmt.Sprintf("%d_Parse", i), func(t *testing.T) {
			s, err := ParseParseMsg(tt.in)
			require.Equal(t, tt.out, s)
			require.ErrorIs(t, err, tt.e)
		})
	}

}

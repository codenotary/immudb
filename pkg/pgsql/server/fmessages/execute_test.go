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

func TestExecutedMsg(t *testing.T) {
	var tests = []struct {
		in  []byte
		out Execute
		e   error
	}{
		{h.Join([][]byte{h.S("port"), h.I32(2)}),
			Execute{
				"port",
				// Maximum number of rows to return, if portal contains a query that returns rows (ignored otherwise). Zero denotes “no limit”.
				2,
			},
			nil,
		},
		{h.Join([][]byte{}),
			Execute{},
			io.EOF,
		},
		{h.Join([][]byte{h.S("port")}),
			Execute{},
			io.EOF,
		},
	}

	for i, tt := range tests {
		t.Run(fmt.Sprintf("%d_execute", i), func(t *testing.T) {
			s, err := ParseExecuteMsg(tt.in)
			require.Equal(t, tt.out, s)
			require.ErrorIs(t, err, tt.e)
		})
	}

}

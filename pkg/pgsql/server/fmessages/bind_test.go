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
	"math"
	"testing"

	pgserror "github.com/codenotary/immudb/pkg/pgsql/errors"
	h "github.com/codenotary/immudb/pkg/pgsql/server/fmessages/fmessages_test"
	"github.com/codenotary/immudb/pkg/pgsql/server/pgmeta"
	"github.com/stretchr/testify/require"
)

func TestParseBindMsg(t *testing.T) {
	var tests = []struct {
		in  []byte
		out BindMsg
		e   error
	}{
		{h.Join([][]byte{h.S("port"), h.S("st"), h.I16(1), h.I16(0), h.I16(1), h.I32(2), h.I16(1), h.I16(1), h.I16(1)}),
			BindMsg{
				DestPortalName:          "port",
				PreparedStatementName:   "st",
				ParamVals:               []interface{}{"\x00\x01"},
				ResultColumnFormatCodes: []int16{1},
			},
			nil,
		},
		{h.Join([][]byte{h.S("port"), h.S("st"), h.I16(0), h.I16(1), h.I32(2), h.I16(1), h.I16(1), h.I16(1)}),
			BindMsg{
				DestPortalName:          "port",
				PreparedStatementName:   "st",
				ParamVals:               []interface{}{"\x00\x01"},
				ResultColumnFormatCodes: []int16{1},
			},
			nil,
		},
		{h.Join([][]byte{h.S("port"), h.S("st"), h.I16(1), h.I16(1), h.I16(1), h.I32(2), h.I16(1), h.I16(1), h.I16(1)}),
			BindMsg{
				DestPortalName:          "port",
				PreparedStatementName:   "st",
				ParamVals:               []interface{}{h.I16(1)},
				ResultColumnFormatCodes: []int16{1},
			},
			nil,
		},
		{h.Join([][]byte{h.S("port"), h.S("st"), h.I16(2), h.I16(1), h.I16(0), h.I16(2), h.I32(2), h.I16(1), h.I32(2), h.I16(1), h.I16(1), h.I16(1)}),
			BindMsg{
				DestPortalName:          "port",
				PreparedStatementName:   "st",
				ParamVals:               []interface{}{h.I16(1), "\x00\x01"},
				ResultColumnFormatCodes: []int16{1},
			},
			nil,
		},
		{h.Join([][]byte{}),
			BindMsg{},
			io.EOF,
		},
		{h.Join([][]byte{h.S("port")}),
			BindMsg{},
			io.EOF,
		},
		{h.Join([][]byte{h.S("port"), h.S("st")}),
			BindMsg{},
			io.EOF,
		},
		{h.Join([][]byte{h.S("port"), h.S("st"), h.I16(1)}),
			BindMsg{},
			io.EOF,
		},
		{h.Join([][]byte{h.S("port"), h.S("st"), h.I16(1), h.I16(0)}),
			BindMsg{},
			io.EOF,
		},
		{h.Join([][]byte{h.S("port"), h.S("st"), h.I16(1), h.I16(0), h.I16(1)}),
			BindMsg{},
			io.EOF,
		},
		{h.Join([][]byte{h.S("port"), h.S("st"), h.I16(1), h.I16(0), h.I16(1), h.I32(2)}),
			BindMsg{},
			io.EOF,
		},
		{h.Join([][]byte{h.S("port"), h.S("st"), h.I16(1), h.I16(0), h.I16(1), h.I32(2), h.I16(1)}),
			BindMsg{},
			io.EOF,
		},
		{h.Join([][]byte{h.S("port"), h.S("st"), h.I16(1), h.I16(0), h.I16(1), h.I32(2), h.I16(1), h.I16(1)}),
			BindMsg{},
			io.EOF,
		},
		{h.Join([][]byte{h.S("port"), h.S("st"), h.I16(1), h.I16(5), h.I16(1), h.I32(2), h.I16(1), h.I16(1)}),
			BindMsg{},
			pgserror.ErrMalformedMessage,
		},
		{h.Join([][]byte{h.S("port"), h.S("st"), h.I16(2), h.I16(0), h.I16(1), h.I32(2), h.I16(1), h.I16(1)}),
			BindMsg{},
			pgserror.ErrMalformedMessage,
		},
		{h.Join([][]byte{h.S("port"), h.S("st"), h.I16(2), h.I16(1), h.I16(0), h.I16(2), h.I32(math.MaxInt32), h.B(make([]byte, 1024)), h.I32(2), h.I16(1), h.I16(1), h.I16(1)}),
			BindMsg{},
			pgserror.ErrParametersValueSizeTooLarge,
		},
		{h.Join([][]byte{h.S("port"), h.S("st"), h.I16(1), h.I16(1), h.I16(1), h.I32(-1), h.I16(-1), h.I16(1), h.I16(1)}),
			BindMsg{},
			pgserror.ErrNegativeParameterValueLen,
		},
		{h.Join([][]byte{h.S("port"), h.S("st"), h.I16(-1), h.I16(1), h.I16(1), h.I32(-1), h.I16(-1), h.I16(1), h.I16(1)}),
			BindMsg{},
			pgserror.ErrMalformedMessage,
		},
		{h.Join([][]byte{h.S("port"), h.S("st"), h.I16(1), h.I16(1), h.I16(1), h.I32(2), h.I16(1), h.I16(-1), h.I16(1)}),
			BindMsg{},
			pgserror.ErrMalformedMessage,
		},
		{h.Join([][]byte{h.S("port"), h.S("st"), h.I16(3), h.I16(1), h.I16(1), h.I16(3), h.I16(3), h.I32(2), h.I16(1), h.I32(2), h.I16(1), h.I32(2), h.I16(1), h.I16(1), h.I16(1)}),
			BindMsg{},
			pgserror.ErrMalformedMessage,
		},
	}

	pgmeta.MaxMsgSize = 1024

	for i, tt := range tests {
		t.Run(fmt.Sprintf("%d_bind", i), func(t *testing.T) {
			s, err := ParseBindMsg(tt.in)
			require.Equal(t, tt.out, s)
			require.ErrorIs(t, err, tt.e)
		})
	}

}

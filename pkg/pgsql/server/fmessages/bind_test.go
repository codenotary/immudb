package fmessages

import (
	"fmt"
	h "github.com/codenotary/immudb/pkg/pgsql/server/fmessages/fmessages_test"
	"github.com/stretchr/testify/require"
	"io"
	"testing"
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
	}

	for i, tt := range tests {
		t.Run(fmt.Sprintf("%d_bind", i), func(t *testing.T) {
			s, err := ParseBindMsg(tt.in)
			require.Equal(t, tt.out, s)
			require.Equal(t, tt.e, err)
		})
	}

}

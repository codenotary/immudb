package fmessages

import (
	"fmt"
	h "github.com/codenotary/immudb/pkg/pgsql/server/fmessages/fmessages_test"
	"github.com/stretchr/testify/require"
	"io"
	"testing"
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
			require.Equal(t, tt.e, err)
		})
	}

}

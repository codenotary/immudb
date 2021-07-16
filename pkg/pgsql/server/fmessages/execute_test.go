package fmessages

import (
	"fmt"
	h "github.com/codenotary/immudb/pkg/pgsql/server/fmessages/fmessages_test"
	"github.com/stretchr/testify/require"
	"io"
	"testing"
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
			require.Equal(t, tt.e, err)
		})
	}

}

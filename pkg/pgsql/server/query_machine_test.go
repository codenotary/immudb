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
	"errors"
	"fmt"
	"net"
	"os"
	"testing"

	"github.com/codenotary/immudb/embedded/logger"
	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/codenotary/immudb/pkg/pgsql/server/bmessages"
	h "github.com/codenotary/immudb/pkg/pgsql/server/fmessages/fmessages_test"
	"github.com/stretchr/testify/require"
)

func TestSession_QueriesMachine(t *testing.T) {
	var tests = []struct {
		name       string
		in         func(conn net.Conn)
		out        error
		portals    map[string]*portal
		statements map[string]*statement
	}{
		{
			name: "unsupported message",
			in: func(c2 net.Conn) {
				ready4Query := make([]byte, len(bmessages.ReadyForQuery()))
				c2.Read(ready4Query)
				//unsupported message
				c2.Write([]byte("_"))
				unsupported := make([]byte, 500)
				c2.Read(unsupported)
				c2.Close()
			},
			out: nil,
		},
		{
			name: "fail first ready for query message",
			in: func(c2 net.Conn) {
				c2.Close()
			},
			out: errors.New("io: read/write on closed pipe"),
		},
		{
			name: "connection is closed",
			in: func(c2 net.Conn) {
				ready4Query := make([]byte, len(bmessages.ReadyForQuery()))
				c2.Read(ready4Query)
				c2.Close()
			},
			out: nil,
		},
		{
			name: "wait for sync",
			in: func(c2 net.Conn) {
				ready4Query := make([]byte, len(bmessages.ReadyForQuery()))
				c2.Read(ready4Query)
				//parse message
				c2.Write(h.Msg('P', h.Join([][]byte{h.S("st"), h.S("set test"), h.I16(1), h.I32(0)})))
				ready4Query = make([]byte, len(bmessages.ReadyForQuery()))
				c2.Read(ready4Query)
				c2.Write(h.Msg('B', h.Join([][]byte{h.S("port"), h.S("wrong_st"), h.I16(1), h.I16(0), h.I16(1), h.I32(2), h.I16(1), h.I16(1), h.I16(1)})))
				errst := make([]byte, 500)
				c2.Read(errst)
				c2.Write(h.Msg('B', h.Join([][]byte{h.S("port"), h.S("st"), h.I16(1), h.I16(0), h.I16(1), h.I32(2), h.I16(1), h.I16(1), h.I16(1)})))
				c2.Write(h.Msg('S', []byte{0}))
				ready4Query = make([]byte, len(bmessages.ReadyForQuery()))
				c2.Read(ready4Query)
				// Terminate message
				c2.Write(h.Msg('X', []byte{0}))
			},
			out: nil,
		},
		{
			name: "error on parse-infer parameters",
			in: func(c2 net.Conn) {
				ready4Query := make([]byte, len(bmessages.ReadyForQuery()))
				c2.Read(ready4Query)
				//parse message
				c2.Write(h.Msg('P', h.Join([][]byte{h.S("st"), h.S("wrong statement"), h.I16(1), h.I32(0)})))
				errst := make([]byte, 500)
				c2.Read(errst)
				// Terminate message
				c2.Write(h.Msg('X', []byte{0}))

			},
			out: nil,
		},
		{
			name: "statement already present",
			in: func(c2 net.Conn) {
				ready4Query := make([]byte, len(bmessages.ReadyForQuery()))
				c2.Read(ready4Query)
				c2.Write(h.Msg('P', h.Join([][]byte{h.S("st"), h.S("set test"), h.I16(1), h.I32(0)})))
				ready4Query = make([]byte, len(bmessages.ReadyForQuery()))
				c2.Read(ready4Query)
				//parse message
				c2.Write(h.Msg('P', h.Join([][]byte{h.S("st"), h.S("set test"), h.I16(1), h.I32(0)})))
				errst := make([]byte, 500)
				c2.Read(errst)
				// Terminate message
				c2.Write(h.Msg('X', []byte{0}))

			},
			out: nil,
		},
		{
			name: "error on parse complete",
			in: func(c2 net.Conn) {
				ready4Query := make([]byte, len(bmessages.ReadyForQuery()))
				c2.Read(ready4Query)
				//parse message
				c2.Write(h.Msg('P', h.Join([][]byte{h.S("st"), h.S("set test"), h.I16(1), h.I32(0)})))
				c2.Close()
			},
			out: nil,
		},
		{
			name: "describe S statement not found",
			in: func(c2 net.Conn) {
				ready4Query := make([]byte, len(bmessages.ReadyForQuery()))
				c2.Read(ready4Query)
				c2.Write(h.Msg('P', h.Join([][]byte{h.S("st"), h.S("set test"), h.I16(1), h.I32(0)})))
				ready4Query = make([]byte, len(bmessages.ReadyForQuery()))
				c2.Read(ready4Query)
				c2.Write(h.Msg('D', h.Join([][]byte{{'S'}, h.S("wrong st")})))
				c2.Close()
			},
			out: nil,
		},
		{
			name: "describe S ParameterDescription error",
			in: func(c2 net.Conn) {
				ready4Query := make([]byte, len(bmessages.ReadyForQuery()))
				c2.Read(ready4Query)
				c2.Write(h.Msg('P', h.Join([][]byte{h.S("st"), h.S("set test"), h.I16(1), h.I32(0)})))
				ready4Query = make([]byte, len(bmessages.ReadyForQuery()))
				c2.Read(ready4Query)
				c2.Write(h.Msg('D', h.Join([][]byte{{'S'}, h.S("st")})))
				c2.Close()
			},
			out: nil,
		},
		{
			name: "describe S RowDescription error",
			in: func(c2 net.Conn) {
				ready4Query := make([]byte, len(bmessages.ReadyForQuery()))
				c2.Read(ready4Query)
				c2.Write(h.Msg('P', h.Join([][]byte{h.S("st"), h.S("set test"), h.I16(1), h.I32(0)})))
				ready4Query = make([]byte, len(bmessages.ReadyForQuery()))
				c2.Read(ready4Query)
				c2.Write(h.Msg('D', h.Join([][]byte{{'S'}, h.S("st")})))
				rowDescription := make([]byte, len(bmessages.ParameterDescription(nil)))
				c2.Read(rowDescription)
				c2.Close()
			},
			out: nil,
		},
		{
			name: "describe P portal not found",
			in: func(c2 net.Conn) {
				ready4Query := make([]byte, len(bmessages.ReadyForQuery()))
				c2.Read(ready4Query)
				c2.Write(h.Msg('P', h.Join([][]byte{h.S("st"), h.S("set test"), h.I16(1), h.I32(0)})))
				ready4Query = make([]byte, len(bmessages.ReadyForQuery()))
				c2.Read(ready4Query)
				c2.Write(h.Msg('D', h.Join([][]byte{{'P'}, h.S("port")})))
				c2.Close()
			},
			out: nil,
		},
		{
			name: "describe P row desc error",
			in: func(c2 net.Conn) {
				ready4Query := make([]byte, len(bmessages.ReadyForQuery()))
				c2.Read(ready4Query)
				//parse message
				c2.Write(h.Msg('P', h.Join([][]byte{h.S("st"), h.S(";"), h.I16(1), h.I32(0)})))
				ready4Query = make([]byte, len(bmessages.ReadyForQuery()))
				c2.Read(ready4Query)
				c2.Write(h.Msg('B', h.Join([][]byte{h.S("port"), h.S("st"), h.I16(1), h.I16(0), h.I16(1), h.I32(2), h.I16(1), h.I16(1), h.I16(1)})))
				bindComplete := make([]byte, len(bmessages.BindComplete()))
				c2.Read(bindComplete)
				c2.Write(h.Msg('S', []byte{0}))
				ready4Query = make([]byte, len(bmessages.ReadyForQuery()))
				c2.Read(ready4Query)
				c2.Write(h.Msg('D', h.Join([][]byte{{'P'}, h.S("port")})))
				c2.Close()
			},
			out: nil,
		},
		{
			name: "sync error",
			in: func(c2 net.Conn) {
				ready4Query := make([]byte, len(bmessages.ReadyForQuery()))
				c2.Read(ready4Query)
				c2.Write(h.Msg('S', []byte{0}))
				c2.Close()
			},
			out: nil,
		},
		{
			name: "query results error",
			in: func(c2 net.Conn) {
				ready4Query := make([]byte, len(bmessages.ReadyForQuery()))
				c2.Read(ready4Query)
				c2.Write(h.Msg('Q', h.S("_wrong_")))
				c2.Close()
			},
			out: nil,
		},
		{
			name: "query command complete error",
			in: func(c2 net.Conn) {
				ready4Query := make([]byte, len(bmessages.ReadyForQuery()))
				c2.Read(ready4Query)
				c2.Write(h.Msg('Q', h.S("set test")))
				c2.Close()
			},
			out: nil,
		},
		{
			name: "query command ready for query error",
			in: func(c2 net.Conn) {
				ready4Query := make([]byte, len(bmessages.ReadyForQuery()))
				c2.Read(ready4Query)
				c2.Write(h.Msg('Q', h.S("set test")))
				cc := make([]byte, len(bmessages.CommandComplete([]byte(`ok`))))
				c2.Read(cc)
				c2.Close()
			},
			out: nil,
		},
		{
			name: "bind portal already present error",
			in: func(c2 net.Conn) {
				ready4Query := make([]byte, len(bmessages.ReadyForQuery()))
				c2.Read(ready4Query)
				//parse message
				c2.Write(h.Msg('P', h.Join([][]byte{h.S("st"), h.S("set test"), h.I16(1), h.I32(0)})))
				ready4Query = make([]byte, len(bmessages.ReadyForQuery()))
				c2.Read(ready4Query)
				c2.Write(h.Msg('B', h.Join([][]byte{h.S("port"), h.S("st"), h.I16(1), h.I16(0), h.I16(1), h.I32(2), h.I16(1), h.I16(1), h.I16(1)})))
				bindComplete := make([]byte, len(bmessages.BindComplete()))
				c2.Read(bindComplete)
				c2.Write(h.Msg('B', h.Join([][]byte{h.S("port"), h.S("st"), h.I16(1), h.I16(0), h.I16(1), h.I32(2), h.I16(1), h.I16(1), h.I16(1)})))
				bindComplete = make([]byte, len(bmessages.BindComplete()))
				c2.Read(bindComplete)
				c2.Close()
			},
			out: nil,
		},
		{
			name: "bind named param error",
			in: func(c2 net.Conn) {
				ready4Query := make([]byte, len(bmessages.ReadyForQuery()))
				c2.Read(ready4Query)
				c2.Write(h.Msg('B', h.Join([][]byte{h.S("port"), h.S("st"), h.I16(1), h.I16(0), h.I16(1), h.I32(2), h.I16(1), h.I16(1), h.I16(1)})))
				c2.Close()
			},
			out: nil,
			statements: map[string]*statement{
				"st": {
					Name:         "st",
					SQLStatement: "test",
					PreparedStmt: nil,
					Params: []*schema.Column{{
						Name: "test",
						Type: "INTEGER",
					}},
					Results: nil,
				},
			},
		},
		{
			name: "bind complete error",
			in: func(c2 net.Conn) {
				ready4Query := make([]byte, len(bmessages.ReadyForQuery()))
				c2.Read(ready4Query)
				//parse message
				c2.Write(h.Msg('P', h.Join([][]byte{h.S("st"), h.S("set set"), h.I16(1), h.I32(0)})))
				ready4Query = make([]byte, len(bmessages.ReadyForQuery()))
				c2.Read(ready4Query)
				c2.Write(h.Msg('B', h.Join([][]byte{h.S("port"), h.S("st"), h.I16(1), h.I16(0), h.I16(1), h.I32(2), h.I16(1), h.I16(1), h.I16(1)})))
				c2.Close()
			},
			out: nil,
		},
		{
			name: "execute write result error",
			in: func(c2 net.Conn) {
				ready4Query := make([]byte, len(bmessages.ReadyForQuery()))
				c2.Read(ready4Query)
				c2.Write(h.Msg('E', h.Join([][]byte{h.S("port"), h.I32(1)})))
				c2.Close()
			},
			out: nil,
			portals: map[string]*portal{
				"port": {
					Statement: &statement{
						SQLStatement: "test",
					},
					Parameters: []*schema.NamedParam{
						{
							Name: "test",
						},
					},
					ResultColumnFormatCodes: []int16{1},
				},
			},
		},
		{
			name: "execute command complete error",
			in: func(c2 net.Conn) {
				ready4Query := make([]byte, len(bmessages.ReadyForQuery()))
				c2.Read(ready4Query)
				//parse message
				c2.Write(h.Msg('P', h.Join([][]byte{h.S("st"), h.S("set set"), h.I16(1), h.I32(0)})))
				ready4Query = make([]byte, len(bmessages.ReadyForQuery()))
				c2.Read(ready4Query)
				c2.Write(h.Msg('B', h.Join([][]byte{h.S("port"), h.S("st"), h.I16(1), h.I16(0), h.I16(1), h.I32(2), h.I16(1), h.I16(1), h.I16(1)})))
				bindComplete := make([]byte, len(bmessages.BindComplete()))
				c2.Read(bindComplete)
				c2.Write(h.Msg('E', h.Join([][]byte{h.S("port"), h.I32(1)})))
				c2.Close()
			},
			out: nil,
		},
		{
			name: "execute command complete error",
			in: func(c2 net.Conn) {
				ready4Query := make([]byte, len(bmessages.ReadyForQuery()))
				c2.Read(ready4Query)
				//parse message
				c2.Write(h.Msg('P', h.Join([][]byte{h.S("st"), h.S("set set"), h.I16(1), h.I32(0)})))
				ready4Query = make([]byte, len(bmessages.ReadyForQuery()))
				c2.Read(ready4Query)
				c2.Write(h.Msg('B', h.Join([][]byte{h.S("port"), h.S("st"), h.I16(1), h.I16(0), h.I16(1), h.I32(2), h.I16(1), h.I16(1), h.I16(1)})))
				bindComplete := make([]byte, len(bmessages.BindComplete()))
				c2.Read(bindComplete)
				c2.Write(h.Msg('E', h.Join([][]byte{h.S("port"), h.I32(1)})))
				c2.Close()
			},
			out: nil,
		},
		{
			name: "version info error",
			in: func(c2 net.Conn) {
				ready4Query := make([]byte, len(bmessages.ReadyForQuery()))
				c2.Read(ready4Query)
				c2.Write(h.Msg('Q', h.S("select version()")))
				c2.Close()
			},
			out: nil,
		},
		{
			name: "flush",
			in: func(c2 net.Conn) {
				ready4Query := make([]byte, len(bmessages.ReadyForQuery()))
				c2.Read(ready4Query)
				c2.Write(h.Msg('H', nil))
				c2.Close()
			},
			out: nil,
		},
		{
			name: "schema info",
			in: func(c2 net.Conn) {
				ready4Query := make([]byte, len(bmessages.ReadyForQuery()))
				c2.Read(ready4Query)
				c2.Write(h.Msg('Q', h.S("select current_schema()")))
				c2.Close()
			},
			out: nil,
		},
		{
			name: "table help",
			in: func(c2 net.Conn) {
				ready4Query := make([]byte, len(bmessages.ReadyForQuery()))
				c2.Read(ready4Query)
				c2.Write(h.Msg('Q', h.S(tableHelpPrefix)))
				c2.Close()
			},
			out: nil,
		},
	}

	for i, tt := range tests {
		t.Run(fmt.Sprintf("qm scenario %d: %s", i, tt.name), func(t *testing.T) {

			c1, c2 := net.Pipe()

			mr := &messageReader{
				conn: c1,
			}

			s := session{
				log:        logger.NewSimpleLogger("test", os.Stdout),
				mr:         mr,
				statements: make(map[string]*statement),
				portals:    make(map[string]*portal),
			}

			if tt.statements != nil {
				s.statements = tt.statements
			}
			if tt.portals != nil {
				s.portals = tt.portals
			}
			go tt.in(c2)

			err := s.QueryMachine()

			require.Equal(t, tt.out, err)
		})
	}
}

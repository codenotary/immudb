/*
Copyright 2026 Codenotary Inc. All rights reserved.

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
	"encoding/binary"
	"io"
	"math"
	"net"
	"testing"

	"github.com/codenotary/immudb/pkg/pgsql/errors"
	"github.com/codenotary/immudb/pkg/pgsql/server/pgmeta"
	"github.com/stretchr/testify/require"
)

func TestSession_MessageReader(t *testing.T) {
	// Each block uses its own c1/c2 pair and the goroutine captures the
	// peer connection by value (function parameter) instead of by
	// closure over the outer variable.  Reusing a single `c2` variable
	// across blocks and closing it inside a goroutine created a race
	// where the previous goroutine's c2.Close() could fire after the
	// main goroutine reassigned c2 to a fresh pipe, closing the new
	// pipe prematurely and surfacing as spurious EOF in CI.
	{
		c1, c2 := net.Pipe()
		mr := &messageReader{conn: c1}

		go func(c net.Conn) {
			c.Write([]byte{'E'})
			c.Close()
		}(c2)

		_, err := mr.ReadRawMessage()
		require.ErrorIs(t, err, io.EOF)
	}

	{
		c1, c2 := net.Pipe()
		mr := &messageReader{conn: c1}

		go func(c net.Conn) {
			c.Write([]byte{'E'})
			c.Write([]byte{0, 0, 0, 4})
			c.Close()
		}(c2)

		_, err := mr.ReadRawMessage()
		require.NoError(t, err)

		mr = &messageReader{}
		err = mr.CloseConnection()
		require.NoError(t, err)
	}

	{
		c1, c2 := net.Pipe()
		mr := &messageReader{conn: c1}

		b := make([]byte, 4)
		binary.BigEndian.PutUint32(b, math.MaxUint32)

		go func(c net.Conn) {
			c.Write([]byte{'E'})
			c.Write(b)
			c.Close()
		}(c2)

		_, err := mr.ReadRawMessage()
		require.ErrorIs(t, err, errors.ErrMalformedMessage)

		mr = &messageReader{}
		err = mr.CloseConnection()
		require.NoError(t, err)
	}
}

func TestSession_MessageReaderMaxMsgSize(t *testing.T) {

	c1, c2 := net.Pipe()
	mr := &messageReader{
		conn: c1,
	}
	b := make([]byte, 4)
	binary.BigEndian.PutUint32(b, uint32(pgmeta.MaxMsgSize))
	go func() {
		c2.Write([]byte{'E'})
		c2.Write(b)
		c2.Close()
	}()

	_, err := mr.ReadRawMessage()

	require.ErrorIs(t, err, io.EOF)

	mr = &messageReader{}
	err = mr.CloseConnection()

	require.NoError(t, err)
}

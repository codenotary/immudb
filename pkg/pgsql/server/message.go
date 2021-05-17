/*
Copyright 2021 CodeNotary, Inc. All rights reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

	http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package server

import (
	"encoding/binary"
	"fmt"
	"github.com/codenotary/immudb/pkg/pgsql/server/pgmeta"
	"net"
)

type rawMessage struct {
	t       byte
	payload []byte
}

type messageReader struct {
	conn net.Conn
}

type MessageReader interface {
	ReadRawMessage() (*rawMessage, error)
	Write(msg []byte) (int, error)
	Read(data []byte) (int, error)
	UpgradeConnection(conn net.Conn)
	CloseConnection() error
	Connection() net.Conn
}

func NewMessageReader(conn net.Conn) *messageReader {
	return &messageReader{conn: conn}
}

func (r *messageReader) ReadRawMessage() (*rawMessage, error) {
	t := make([]byte, 1)
	if _, err := r.conn.Read(t); err != nil {
		return nil, err
	}

	if _, ok := pgmeta.MTypes[t[0]]; !ok {
		return nil, fmt.Errorf(ErrUnknowMessageType.Error()+". Message first byte was %s", string(t[0]))
	}

	lb := make([]byte, 4)
	if _, err := r.conn.Read(lb); err != nil {
		return nil, err
	}
	l := binary.BigEndian.Uint32(lb)
	payload := make([]byte, l-4)
	if _, err := r.conn.Read(payload); err != nil {
		return nil, err
	}

	return &rawMessage{
		t:       t[0],
		payload: payload,
	}, nil
}

func (r *messageReader) Write(data []byte) (int, error) {
	return r.conn.Write(data)
}

func (r *messageReader) Read(data []byte) (int, error) {
	return r.conn.Read(data)
}

func (r *messageReader) UpgradeConnection(conn net.Conn) {
	r.conn = conn
}

func (r *messageReader) CloseConnection() error {
	if r.conn != nil {
		return r.conn.Close()
	}
	return nil
}

func (r *messageReader) Connection() net.Conn {
	return r.conn
}

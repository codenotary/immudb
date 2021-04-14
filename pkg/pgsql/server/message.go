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
	"net"
)

var Mtypes = map[byte]mtype{
	'Q': "query",
	'T': "rowDescription",
	'D': "dataRow",
	'C': "commandComplete",
	'Z': "readyForQuery",
	'R': "cleartextPassword",
	'p': "PasswordMessage",
	'U': "unknown",
}

type mtype string

type rawMessage struct {
	t       byte
	payload []byte
}

type startupMessage struct {
	payload []byte
}

type messageReader struct {
	conn net.Conn
}

type MessageReader interface {
	ReadStartUpMessage() (*startupMessage, error)
	ReadRawMessage() (*rawMessage, error)
	WriteMessage(func() []byte) (int, error)
}

func NewMessageReader(conn net.Conn) *messageReader {
	return &messageReader{conn: conn}
}

func (r *messageReader) ReadRawMessage() (*rawMessage, error) {
	t := make([]byte, 1)
	if _, err := r.conn.Read(t); err != nil {
		return nil, err
	}
	if _, ok := Mtypes[t[0]]; !ok {
		return nil, ErrUnknowMessageType
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

func (r *messageReader) ReadStartUpMessage() (*startupMessage, error) {
	lb := make([]byte, 4)
	if _, err := r.conn.Read(lb); err != nil {
		return nil, err
	}
	protocolVersion := make([]byte, 4)
	if _, err := r.conn.Read(protocolVersion); err != nil {
		return nil, err
	}
	connString := make([]byte, binary.BigEndian.Uint32(lb)-4)
	if _, err := r.conn.Read(connString); err != nil {
		return nil, err
	}

	return &startupMessage{
		payload: connString,
	}, nil
}

func (r *messageReader) WriteMessage(f func() []byte) (int, error) {
	return r.conn.Write(f())
}

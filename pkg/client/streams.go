/*
Copyright 2019-2020 vChain, Inc.

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

package client

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/pkg/errors"
	"io"
)

const chunkSize int = 64 * 1024 // 64 KiB

var ErrMaxChunkSizeReached = errors.New("max chunk size reached")
var ErrDataLoss = errors.New("some data is not sent to the server")

type KeyValue struct {
	Key   *ValueSize
	Value *ValueSize
}

type ValueSize struct {
	Content *bufio.Reader
	Size    int
}

func (c *immuClient) Stream(ctx context.Context) (schema.ImmuService_StreamClient, error) {
	if !c.IsConnected() {
		return nil, ErrNotConnected
	}
	return c.ServiceClient.Stream(ctx)
}

type kvStreamer struct {
	s schema.ImmuService_StreamClient
	b *bytes.Buffer
}

func NewKvStreamer(s schema.ImmuService_StreamClient) *kvStreamer {
	buffer := bytes.NewBuffer([]byte{})
	return &kvStreamer{
		s: s,
		b: buffer,
	}
}

func (st *kvStreamer) Send(kv *KeyValue) error {
	err := st.send(kv.Key.Content, kv.Key.Size)
	if err != nil {
		return err
	}
	c, err := st.s.Recv()
	if err != nil {
		return err
	}
	err = st.send(kv.Value.Content, kv.Value.Size)
	if err != nil {
		return err
	}

	c, err = st.s.Recv()
	if err != nil {
		return err
	}

	println(string(c.Content))

	return nil
}

func (st *kvStreamer) Recv() ([]byte, error) {
	chunk, err := st.s.Recv()
	if err != nil {
		return nil, err
	}
	return chunk.Content, nil
}

func (st *kvStreamer) Close() error {
	if st.b.Len() > 0 {
		return ErrDataLoss
	}
	return st.s.CloseSend()
}

func (st *kvStreamer) send(reader *bufio.Reader, payloadSize int) (err error) {
	var read = 0
	for {
		// if read is 0 here trailer is created
		if read == 0 {
			ml := make([]byte, 8)
			binary.BigEndian.PutUint64(ml, uint64(payloadSize))
			st.b.Write(ml)
		}
		// read data from reader and append it to the buffer
		data := make([]byte, reader.Size())
		r, err := reader.Read(data)
		if err != nil {
			if err != io.EOF {
				return err
			}
		}

		read += r
		// no more data to read in the reader, exit
		if read == 0 {
			return nil
		}

		// append read data in the buffer
		st.b.Write(data[:r])

		// chunk processing
		var chunk []byte
		// last chunk creation
		if read == payloadSize {
			chunk = make([]byte, st.b.Len())
			if err != nil {
				return nil
			}
			_, err = st.b.Read(chunk)
			if err != nil {
				return nil
			}
		}
		// enough data to send a chunk
		if st.b.Len() > chunkSize {
			chunk = make([]byte, chunkSize)
			_, err = st.b.Read(chunk)
			if err != nil {
				return nil
			}
		}
		// sending ...
		if len(chunk) > 0 {
			err = st.s.Send(&schema.Chunk{
				Content: chunk,
			})
			if err != nil {
				return err
			}
			// is last chunk
			if read == payloadSize {
				return nil
			}
		}
	}
}

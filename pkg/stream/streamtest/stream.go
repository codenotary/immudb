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

package streamtest

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"io"
	"io/ioutil"
	"os"

	"github.com/codenotary/immudb/pkg/api/schema"
)

type ChunkError struct {
	C *schema.Chunk
	E error
}
type ImmuServiceReceiver_StreamMock struct {
	cc    int
	ce    []*ChunkError
	RecvF func() (*schema.Chunk, error)
}

func (ism *ImmuServiceReceiver_StreamMock) Recv() (*schema.Chunk, error) {
	return ism.RecvF()
}

func DefaultImmuServiceReceiverStreamMock(ce []*ChunkError) *ImmuServiceReceiver_StreamMock {
	m := &ImmuServiceReceiver_StreamMock{
		ce: ce,
	}
	f := func() (*schema.Chunk, error) {
		if len(m.ce) > 0 {
			c := m.ce[m.cc].C
			e := m.ce[m.cc].E
			m.cc++
			return c, e
		}
		return nil, nil
	}
	m.RecvF = f
	return m
}

type ImmuServiceSender_StreamMock struct {
	SendF    func(*schema.Chunk) error
	RecvMsgF func(m interface{}) error
}

func DefaultImmuServiceSenderStreamMock() *ImmuServiceSender_StreamMock {
	return &ImmuServiceSender_StreamMock{
		SendF: func(*schema.Chunk) error {
			return nil
		},
		RecvMsgF: func(m interface{}) error {
			return nil
		},
	}
}

func (iss *ImmuServiceSender_StreamMock) Send(c *schema.Chunk) error {
	return iss.SendF(c)
}

func (iss *ImmuServiceSender_StreamMock) RecvMsg(m interface{}) error {
	return iss.RecvMsgF(m)
}

func GetTrailer(payloadSize int) []byte {
	ml := make([]byte, 8)
	binary.BigEndian.PutUint64(ml, uint64(payloadSize))
	return ml
}

func GenerateDummyFile(filename string, size int) (*os.File, error) {
	tmpFile, err := ioutil.TempFile(os.TempDir(), "go-stream-bench-"+filename)
	if err != nil {
		return nil, err
	}

	b := make([]byte, size)
	_, err = rand.Read(b)
	if err != nil {
		return nil, err
	}

	_, err = tmpFile.Write(b)
	if err != nil {
		return nil, err
	}

	tmpFile.Seek(0, io.SeekStart)

	return tmpFile, nil
}

func GetSHA256(r io.Reader) ([]byte, error) {
	h := sha256.New()
	_, err := io.Copy(h, r)
	if err != nil {
		return nil, err
	}
	return h.Sum(nil), nil

}

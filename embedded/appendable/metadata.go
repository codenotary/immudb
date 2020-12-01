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
package appendable

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"io"
)

type Metadata struct {
	data map[string][]byte
}

func NewMetadata(b []byte) *Metadata {
	m := &Metadata{
		data: make(map[string][]byte),
	}
	if b != nil {
		bb := bytes.NewBuffer(b)
		m.ReadFrom(bufio.NewReader(bb))
	}
	return m
}

func (m *Metadata) Bytes() []byte {
	var b bytes.Buffer
	w := bufio.NewWriter(&b)
	m.WriteTo(w)
	w.Flush()
	return b.Bytes()
}

func (m *Metadata) ReadFrom(r io.Reader) (int64, error) {
	lenb, err := readField(r)
	if err != nil {
		return 0, err
	}
	len := int(binary.BigEndian.Uint32(lenb))

	for i := 0; i < len; i++ {
		k, err := readField(r)
		if err != nil {
			return 0, err
		}

		v, err := readField(r)
		if err != nil {
			return 0, err
		}

		m.data[string(k)] = v
	}

	return int64(len), nil
}

func (m *Metadata) WriteTo(w io.Writer) (n int64, err error) {
	lenb := make([]byte, 4)
	binary.BigEndian.PutUint32(lenb, uint32(len(m.data)))
	wn, err := writeField(lenb, w)
	n += int64(wn)

	if err != nil {
		return
	}

	for k, v := range m.data {
		wn, err = writeField([]byte(k), w)
		n += int64(wn)

		if err != nil {
			return
		}

		wn, err = writeField(v, w)
		n += int64(wn)

		if err != nil {
			return
		}
	}

	return
}

func (m *Metadata) PutInt(key string, n int) {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, uint64(n))
	m.Put(key, b)
}

func (m *Metadata) GetInt(key string) (int, bool) {
	v, ok := m.Get(key)
	if !ok {
		return 0, ok
	}
	return int(binary.BigEndian.Uint64(v)), true
}

func (m *Metadata) Put(key string, value []byte) {
	m.data[key] = value
}

func (m *Metadata) Get(key string) ([]byte, bool) {
	v, ok := m.data[key]
	return v, ok
}

func readField(r io.Reader) ([]byte, error) {
	lenb := make([]byte, 4)
	_, err := r.Read(lenb)
	if err != nil {
		return nil, err
	}
	len := binary.BigEndian.Uint32(lenb)

	fb := make([]byte, len)
	_, err = r.Read(fb)
	if err != nil {
		return nil, err
	}

	return fb, nil
}

func writeField(b []byte, w io.Writer) (n int, err error) {
	lenb := make([]byte, 4)
	binary.BigEndian.PutUint32(lenb, uint32(len(b)))
	wn, err := w.Write(lenb)
	n += wn
	if err != nil {
		return n, err
	}

	wn, err = w.Write(b)
	n += wn

	return
}

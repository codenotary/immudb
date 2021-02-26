package stream

import (
	"bufio"
	"bytes"
	"github.com/codenotary/immudb/pkg/api/schema"
	"io"
)

func ParseKV(key []byte, vr *bufio.Reader, chunkSize int) (*schema.Entry, error) {
	value, err := ParseValue(vr, chunkSize)
	if err != nil {
		return nil, err
	}
	return &schema.Entry{
		Key:   key,
		Value: value,
	}, nil
}

func ParseValue(vr *bufio.Reader, chunkSize int) (value []byte, err error) {
	b := bytes.NewBuffer([]byte{})
	vl := 0
	eof := false
	chunk := make([]byte, chunkSize)
	for {
		l, err := vr.Read(chunk)
		if err != nil && err != io.EOF {
			return nil, err
		}
		vl += l
		b.Write(chunk)
		if err == io.EOF || l == 0 {
			eof = err == io.EOF
			break
		}
	}
	value = make([]byte, vl)
	_, err = b.Read(value)
	if err != nil {
		return nil, err
	}
	if eof {
		err = io.EOF
	}
	return value, err
}

package stream

import (
	"bytes"
	"github.com/codenotary/immudb/pkg/api/schema"
	"io"
)

// ParseKV returns an entry from a key and a reader
// @todo should be removed and we should use only ReadValue and explicitly setup the entry if needed
func ParseKV(key []byte, vr io.Reader, chunkSize int) (*schema.Entry, error) {
	value, err := ReadValue(vr, chunkSize)
	if err != nil {
		return nil, err
	}
	return &schema.Entry{
		Key:   key,
		Value: value,
	}, nil
}

// ReadValue returns the complete value from a message
// @todo Michele, move it to streamutils package
func ReadValue(vr io.Reader, chunkSize int) (value []byte, err error) {
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

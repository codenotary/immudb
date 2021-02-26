package stream

import (
	"bufio"
	"bytes"
	"io"

	"github.com/codenotary/immudb/pkg/api/schema"
)

// ParseZEntry ...
func ParseZEntry(key []byte, vr *bufio.Reader, chunkSize int) (*schema.ZEntry, error) {
	b := bytes.NewBuffer([]byte{})
	vl := 0
	chunk := make([]byte, chunkSize)
	for {
		l, err := vr.Read(chunk)
		if err != nil && err != io.EOF {
			return nil, err
		}
		vl += l
		b.Write(chunk)
		if err == io.EOF || l == 0 {
			break
		}
	}
	value := make([]byte, vl)
	_, err := b.Read(value)
	if err != nil {
		return nil, err
	}

	// TODO OGG NOW: parse and populate all the values
	entry := schema.Entry{Key: key, Value: value}

	return &schema.ZEntry{
		Set:   nil,
		Key:   key,
		Entry: &entry,
		Score: 0,
		AtTx:  0,
	}, nil
}

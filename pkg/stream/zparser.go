package stream

import (
	"io"

	"github.com/codenotary/immudb/pkg/api/schema"
)

// ParseZEntry ...
func ParseZEntry(
	set []byte,
	key []byte,
	score float64,
	atTx uint64,
	vr io.Reader,
	chunkSize int,
) (*schema.ZEntry, error) {

	value, err := ReadValue(vr, chunkSize)
	if err != nil {
		return nil, err
	}

	entry := schema.Entry{Key: key, Value: value}

	return &schema.ZEntry{
		Set:   set,
		Key:   key,
		Entry: &entry,
		Score: score,
		AtTx:  atTx,
	}, nil
}

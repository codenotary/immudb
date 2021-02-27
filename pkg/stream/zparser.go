package stream

import (
	"bufio"

	"github.com/codenotary/immudb/pkg/api/schema"
)

// ParseZEntry ...
func ParseZEntry(
	set []byte,
	key []byte,
	score float64,
	vr *bufio.Reader,
	chunkSize int,
) (*schema.ZEntry, error) {

	value, err := ParseValue(vr, chunkSize)
	if err != nil {
		return nil, err
	}

	entry := schema.Entry{Key: key, Value: value}

	return &schema.ZEntry{
		Set:   set,
		Key:   key,
		Entry: &entry,
		Score: score,
		// TODO OGG NOW - check with Michele: shouldn't we also send/receive this:
		AtTx: 0,
	}, nil
}

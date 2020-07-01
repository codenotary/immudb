package client

import (
	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/stretchr/testify/assert"
	"io"
	"strings"
	"testing"
)

func TestBatchRequest(t *testing.T) {
	br := BatchRequest{
		Keys:   []io.Reader{strings.NewReader("key1"), strings.NewReader("key2")},
		Values: []io.Reader{strings.NewReader("val1"), strings.NewReader("val2")},
	}
	kvl, err := br.toKVList()
	assert.Nil(t, err)
	assert.IsType(t, kvl, &schema.KVList{})
}

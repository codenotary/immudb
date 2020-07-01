package client

import (
	"github.com/codenotary/immudb/pkg/client/timestamp"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestTimestampService(t *testing.T) {
	tg, _ := timestamp.NewTdefault()
	tss := NewTimestampService(tg)
	tim := tss.GetTime()
	assert.IsType(t, tim, time.Time{})
}

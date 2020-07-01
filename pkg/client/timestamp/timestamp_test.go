package timestamp

import (
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestDefault(t *testing.T) {
	def, err := NewTdefault()
	assert.Nil(t, err)
	assert.IsType(t, def, &tdefault{})
	ts := def.Now()
	assert.IsType(t, ts, time.Time{})
}

func TestNtp(t *testing.T) {
	nt, err := NewNtp()
	assert.Nil(t, err)
	assert.IsType(t, nt, &ntp{})
	ts := nt.Now()
	assert.IsType(t, ts, time.Time{})
}

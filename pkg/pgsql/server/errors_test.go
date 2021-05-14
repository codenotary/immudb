package server

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestMapPgError(t *testing.T) {
	err := ErrUnknowMessageType
	be := MapPgError(err)
	require.NotNil(t, be)
}

package errors

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestMapPgError(t *testing.T) {
	err := ErrUnknowMessageType
	be := MapPgError(err)
	require.NotNil(t, be)
	err = ErrMaxStmtNumberExceeded
	be = MapPgError(err)
	require.NotNil(t, be)
	err = ErrNoStatementFound
	be = MapPgError(err)
	require.NotNil(t, be)
	err = ErrParametersValueSizeTooLarge
	be = MapPgError(err)
	require.NotNil(t, be)
	err = ErrNegativeParameterValueLen
	be = MapPgError(err)
	require.NotNil(t, be)
	err = ErrMalformedMessage
	be = MapPgError(err)
	require.NotNil(t, be)
}

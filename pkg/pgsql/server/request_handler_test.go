package server

import (
	"errors"
	"net"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestHandleRequestNil(t *testing.T) {
	s := NewSessionMock()
	sf := NewSessionFactoryMock(s)
	srv := New(SessFactory(sf))

	c, _ := net.Pipe()
	err := srv.handleRequest(c)

	require.NoError(t, err)
}

func TestHandleRequestInitializeError(t *testing.T) {
	s := NewSessionMock()
	s.InitializeSessionF = func() error {
		return errors.New("init error")
	}
	sf := NewSessionFactoryMock(s)
	srv := New(SessFactory(sf))

	c, _ := net.Pipe()
	err := srv.handleRequest(c)

	require.Error(t, err)
}

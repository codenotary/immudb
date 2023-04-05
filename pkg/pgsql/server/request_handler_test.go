/*
Copyright 2022 Codenotary Inc. All rights reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

	http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

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
	errInit := errors.New("init error")
	s.InitializeSessionF = func() error {
		return errInit
	}
	sf := NewSessionFactoryMock(s)
	srv := New(SessFactory(sf))

	c, _ := net.Pipe()
	err := srv.handleRequest(c)

	require.ErrorIs(t, err, errInit)
}

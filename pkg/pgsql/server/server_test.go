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
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSrv_Initialize(t *testing.T) {
	s := pgsrv{
		port: 99999999999999999,
	}
	err := s.Initialize()
	require.ErrorContains(t, err, "invalid port")
}

func TestSrv_GetPort(t *testing.T) {
	s := pgsrv{}
	err := s.GetPort()
	require.Equal(t, 0, err)
}

func TestSrv_Stop(t *testing.T) {
	s := pgsrv{}
	res := s.Stop()
	require.Nil(t, res)
}

func TestSrv_Serve(t *testing.T) {
	s := pgsrv{}
	err := s.Serve()
	require.ErrorContains(t, err, "no listener found for pgsql server")
}

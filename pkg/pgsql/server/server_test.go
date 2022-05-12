/*
Copyright 2022 CodeNotary, Inc. All rights reserved.

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
	"github.com/stretchr/testify/require"
	"testing"
)

func TestSrv_Initialize(t *testing.T) {
	s := srv{
		Port: 99999999999999999,
	}
	err := s.Initialize()
	require.Error(t, err)
}

func TestSrv_GetPort(t *testing.T) {
	s := srv{}
	err := s.GetPort()
	require.Equal(t, 0, err)
}

func TestSrv_Stop(t *testing.T) {
	s := srv{}
	res := s.Stop()
	require.Nil(t, res)
}

func TestSrv_Serve(t *testing.T) {
	s := srv{}
	err := s.Serve()
	require.Error(t, err)
}

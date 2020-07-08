/*
Copyright 2019-2020 vChain, Inc.

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

package gw

import (
	"errors"
	"testing"

	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/codenotary/immudb/pkg/logger"
	"github.com/stretchr/testify/require"
)

func TestImmuGwServerMock(t *testing.T) {
	igsm := new(ImmuGwServerMock)

	startErr := errors.New("some start error")
	require.Nil(t, igsm.Start())
	igsm.StartF = func() error {
		return startErr
	}
	require.Equal(t, startErr, igsm.Start())

	stopErr := errors.New("some stop error")
	require.Nil(t, igsm.Stop())
	igsm.StopF = func() error {
		return stopErr
	}
	require.Equal(t, stopErr, igsm.Stop())

	withClientCalled := false
	require.Equal(t, igsm, igsm.WithClient(nil))
	igsm.WithClientF = func(isc schema.ImmuServiceClient) ImmuGw {
		withClientCalled = true
		return igsm
	}
	igsm.WithClient(nil)
	require.True(t, withClientCalled)

	withLoggerCalled := false
	require.Equal(t, igsm, igsm.WithLogger(nil))
	igsm.WithLoggerF = func(logger.Logger) ImmuGw {
		withLoggerCalled = true
		return igsm
	}
	igsm.WithLogger(nil)
	require.True(t, withLoggerCalled)

	withOptionsCalled := false
	defaultOpts := DefaultOptions()
	require.Equal(t, igsm, igsm.WithOptions(defaultOpts))
	igsm.WithOptionsF = func(Options) ImmuGw {
		withOptionsCalled = true
		return igsm
	}
	igsm.WithOptions(defaultOpts)
	require.True(t, withOptionsCalled)
}

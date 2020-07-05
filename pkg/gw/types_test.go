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
	"os"
	"testing"

	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/codenotary/immudb/pkg/logger"
	"github.com/stretchr/testify/require"
)

func TestTypes(t *testing.T) {
	server := DefaultServer()
	require.NotNil(t, server.Logger)
	slogger, ok := server.Logger.(*logger.SimpleLogger)
	require.True(t, ok)
	require.Equal(t, os.Stderr, slogger.Logger.Writer())
	defaultOptions := DefaultOptions()
	require.Equal(t, server.Options.Address, defaultOptions.Address)
	require.NotNil(t, server.quit)

	require.Nil(t, server.Client)
	server.WithClient(schema.NewImmuServiceClient(nil))
	require.NotNil(t, server.Client)

	server.Logger = nil
	server.WithLogger(slogger)
	require.NotNil(t, server.Logger)

	server.WithOptions(defaultOptions.WithPort(1111))
	require.Equal(t, 1111, server.Options.Port)
}

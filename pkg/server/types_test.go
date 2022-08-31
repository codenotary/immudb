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
	"io/ioutil"
	"os"
	"testing"

	"github.com/codenotary/immudb/pkg/database"
	"github.com/codenotary/immudb/pkg/stream"
	"github.com/stretchr/testify/require"
)

func TestWithLogger(t *testing.T) {
	dir, err := ioutil.TempDir("", "server_test")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	logger := &mockLogger{}

	s := DefaultServer()
	s.WithOptions(DefaultOptions().WithDir(dir))
	require.NotSame(t, logger, s.Logger)

	s.WithLogger(logger)
	require.Same(t, logger, s.Logger)
}

func TestWithStreamServiceFactory(t *testing.T) {
	dir, err := ioutil.TempDir("", "server_test")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	streamServiceFactory := stream.NewStreamServiceFactory(4096)

	s := DefaultServer()
	s.WithOptions(DefaultOptions().WithDir(dir))
	require.NotSame(t, streamServiceFactory, s.StreamServiceFactory)

	s.WithStreamServiceFactory(streamServiceFactory)
	require.Same(t, streamServiceFactory, s.StreamServiceFactory)
}

func TestWithDbList(t *testing.T) {
	dir, err := ioutil.TempDir("", "server_test")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	dbList := database.NewDatabaseList()

	s := DefaultServer()
	s.WithOptions(DefaultOptions().WithDir(dir))
	require.NotSame(t, dbList, s.dbList)

	s.WithDbList(dbList)
	require.Same(t, dbList, s.dbList)
}

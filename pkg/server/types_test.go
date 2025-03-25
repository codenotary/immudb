/*
Copyright 2025 Codenotary Inc. All rights reserved.

SPDX-License-Identifier: BUSL-1.1
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    https://mariadb.com/bsl11/

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package server

import (
	"testing"

	"github.com/codenotary/immudb/pkg/database"
	"github.com/codenotary/immudb/pkg/stream"
	"github.com/stretchr/testify/require"
)

func TestWithLogger(t *testing.T) {
	dir := t.TempDir()

	logger := &mockLogger{}

	s := DefaultServer()
	s.WithOptions(DefaultOptions().WithDir(dir))
	require.NotSame(t, logger, s.Logger)

	s.WithLogger(logger)
	require.Same(t, logger, s.Logger)
}

func TestWithStreamServiceFactory(t *testing.T) {
	dir := t.TempDir()

	streamServiceFactory := stream.NewStreamServiceFactory(4096)

	s := DefaultServer()
	s.WithOptions(DefaultOptions().WithDir(dir))
	require.NotSame(t, streamServiceFactory, s.StreamServiceFactory)

	s.WithStreamServiceFactory(streamServiceFactory)
	require.Same(t, streamServiceFactory, s.StreamServiceFactory)
}

func TestWithDbList(t *testing.T) {
	dir := t.TempDir()

	dbList := database.NewDatabaseList(nil)

	s := DefaultServer()
	s.WithOptions(DefaultOptions().WithDir(dir))
	require.NotSame(t, dbList, s.dbList)

	s.WithDbList(dbList)
	require.Same(t, dbList, s.dbList)
}

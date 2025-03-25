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
	"context"
	"testing"

	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/codenotary/immudb/pkg/auth"
	"github.com/stretchr/testify/require"
)

func TestServerMultidbHandler(t *testing.T) {
	dir := t.TempDir()

	serverOptions := DefaultOptions().
		WithDir(dir).
		WithMetricsServer(false).
		WithAdminPassword(auth.SysAdminPassword)

	s, closer := testServer(serverOptions)
	defer closer()

	s.Initialize()

	r := &schema.LoginRequest{
		User:     []byte(auth.SysAdminUsername),
		Password: []byte(auth.SysAdminPassword),
	}
	_, err := s.Login(context.Background(), r)
	require.NoError(t, err)

	multidbHandler := &multidbHandler{s: s}

	err = multidbHandler.UseDatabase(context.Background(), "defaultdb")
	require.Error(t, err)

	_, err = multidbHandler.ListDatabases(context.Background())
	require.Error(t, err)

	_, err = multidbHandler.ListUsers(context.Background())
	require.Error(t, err)

	err = multidbHandler.CreateUser(context.Background(), "user1", "user1Password!", "READ")
	require.Error(t, err)

	err = multidbHandler.AlterUser(context.Background(), "user1", "user1Password!", "READWRITE")
	require.Error(t, err)
}

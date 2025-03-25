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
	"time"

	"github.com/codenotary/immudb/embedded/logger"
	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/codenotary/immudb/pkg/auth"
	"github.com/codenotary/immudb/pkg/database"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/metadata"
)

func TestServerTruncator(t *testing.T) {
	serverOptions := DefaultOptions().
		WithDir(t.TempDir()).
		WithMetricsServer(false).
		WithAdminPassword(auth.SysAdminPassword).
		WithAuth(true).
		WithPort(3324)

	s, closer := testServer(serverOptions)
	defer closer()

	err := s.Initialize()
	require.NoError(t, err)

	r := &schema.LoginRequest{
		User:     []byte(auth.SysAdminUsername),
		Password: []byte(auth.SysAdminPassword),
	}

	ctx := context.Background()
	lr, err := s.Login(ctx, r)
	require.NoError(t, err)

	md := metadata.Pairs("authorization", lr.Token)
	ctx = metadata.NewIncomingContext(context.Background(), md)

	dt := 24 * time.Hour
	newdb := &schema.CreateDatabaseRequest{
		Name: "db",
		Settings: &schema.DatabaseNullableSettings{
			TruncationSettings: &schema.TruncationNullableSettings{
				RetentionPeriod:     &schema.NullableMilliseconds{Value: dt.Milliseconds()},
				TruncationFrequency: &schema.NullableMilliseconds{Value: dt.Milliseconds()},
			},
		},
	}

	_, err = s.CreateDatabaseV2(ctx, newdb)
	require.NoError(t, err)

	db, err := s.dbList.GetByName("db")
	require.NoError(t, err)
	dbOpts, err := s.loadDBOptions("db", false)
	require.NoError(t, err)

	err = s.startTruncatorFor(db, dbOpts)
	require.ErrorIs(t, err, database.ErrTruncatorAlreadyRunning)

	err = s.stopTruncatorFor(db.GetName())
	require.NoError(t, err)

	err = s.stopTruncatorFor("db2")
	require.ErrorIs(t, err, ErrTruncatorNotInProgress)

	err = s.startTruncatorFor(db, dbOpts)
	require.NoError(t, err)

	_, err = s.getTruncatorFor(db.GetName())
	require.NoError(t, err)

	s.stopTruncation()

	_, err = s.getTruncatorFor("db3")
	require.ErrorIs(t, ErrTruncatorDoesNotExist, err)
}

func TestServerLoadDatabaseWithRetention(t *testing.T) {
	dir := t.TempDir()

	serverOptions := DefaultOptions().
		WithDir(dir).
		WithMetricsServer(false).
		WithAdminPassword(auth.SysAdminPassword)

	s := DefaultServer().WithOptions(serverOptions).(*ImmuServer)

	s.Initialize()

	r := &schema.LoginRequest{
		User:     []byte(auth.SysAdminUsername),
		Password: []byte(auth.SysAdminPassword),
	}
	ctx := context.Background()
	lr, err := s.Login(ctx, r)
	require.NoError(t, err)

	md := metadata.Pairs("authorization", lr.Token)
	ctx = metadata.NewIncomingContext(context.Background(), md)

	dt := 24 * time.Hour
	newdb := &schema.CreateDatabaseRequest{
		Name: testDatabase,
		Settings: &schema.DatabaseNullableSettings{
			TruncationSettings: &schema.TruncationNullableSettings{
				RetentionPeriod:     &schema.NullableMilliseconds{Value: dt.Milliseconds()},
				TruncationFrequency: &schema.NullableMilliseconds{Value: dt.Milliseconds()},
			},
		},
	}
	_, err = s.CreateDatabaseV2(ctx, newdb)
	require.NoError(t, err)

	err = s.CloseDatabases()
	require.NoError(t, err)

	s.dbList = database.NewDatabaseList(database.NewDBManager(func(name string, opts *database.Options) (database.DB, error) {
		return database.OpenDB(name, s.multidbHandler(), opts, s.Logger)
	}, 10, logger.NewMemoryLogger()))
	s.sysDB = nil

	t.Run("attempt to load database should pass", func(t *testing.T) {
		err = s.loadSystemDatabase(s.Options.Dir, nil, auth.SysAdminPassword, false)
		require.NoError(t, err)

		err = s.loadDefaultDatabase(s.Options.Dir, nil)
		require.NoError(t, err)

		err = s.loadUserDatabases(s.Options.Dir, nil)
		require.NoError(t, err)
	})

	t.Run("attempt to unload user database should pass", func(t *testing.T) {
		_, err = s.UnloadDatabase(ctx, &schema.UnloadDatabaseRequest{Database: testDatabase})
		require.NoError(t, err)
	})
}

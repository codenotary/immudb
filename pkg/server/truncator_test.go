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
	"context"
	"testing"
	"time"

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
			RetentionPeriod:     &schema.NullableMilliseconds{Value: dt.Milliseconds()},
			TruncationFrequency: &schema.NullableMilliseconds{Value: dt.Milliseconds()},
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

	s.stopTruncation()
}

/*
Copyright 2026 Codenotary Inc. All rights reserved.

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
	"fmt"
	"testing"

	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/codenotary/immudb/pkg/auth"
	"github.com/codenotary/immudb/pkg/database"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/types/known/emptypb"
)

func TestServerDatabaseRuntime(t *testing.T) {
	dir := t.TempDir()

	opts := DefaultOptions().WithDir(dir)

	s := DefaultServer()

	s.WithOptions(opts)

	s.Initialize()

	ctx := context.Background()

	resp, err := s.OpenSession(ctx, &schema.OpenSessionRequest{
		Username:     []byte(auth.SysAdminUsername),
		Password:     []byte(auth.SysAdminPassword),
		DatabaseName: DefaultDBName,
	})
	require.NoError(t, err)

	ctx = metadata.NewIncomingContext(context.Background(), metadata.New(map[string]string{"sessionid": resp.GetSessionID()}))

	t.Run("reserved databases can not be updated", func(t *testing.T) {
		_, err = s.UpdateDatabaseV2(ctx, &schema.UpdateDatabaseRequest{
			Database: SystemDBName,
			Settings: &schema.DatabaseNullableSettings{
				Autoload: &schema.NullableBool{Value: false},
			},
		})
		require.ErrorIs(t, err, ErrReservedDatabase)

		_, err = s.UpdateDatabaseV2(ctx, &schema.UpdateDatabaseRequest{
			Database: DefaultDBName,
			Settings: &schema.DatabaseNullableSettings{
				Autoload: &schema.NullableBool{Value: false},
			},
		})
		require.ErrorIs(t, err, ErrReservedDatabase)
	})

	t.Run("user created databases can be updated", func(t *testing.T) {
		_, err = s.CreateDatabaseV2(ctx, &schema.CreateDatabaseRequest{
			Name: "db1",
		})
		require.NoError(t, err)

		_, err = s.UseDatabase(ctx, &schema.Database{DatabaseName: "db1"})
		require.NoError(t, err)

		res, err := s.GetDatabaseSettingsV2(ctx, &schema.DatabaseSettingsRequest{})
		require.NoError(t, err)
		require.NotNil(t, res)
		require.Equal(t, "db1", res.Database)
		require.True(t, res.Settings.Autoload.GetValue())

		_, err = s.UpdateDatabaseV2(ctx, &schema.UpdateDatabaseRequest{
			Database: "db1",
			Settings: &schema.DatabaseNullableSettings{
				Autoload: &schema.NullableBool{Value: false},
			},
		})
		require.NoError(t, err)

		res, err = s.GetDatabaseSettingsV2(ctx, &schema.DatabaseSettingsRequest{})
		require.NoError(t, err)
		require.NotNil(t, res)
		require.Equal(t, "db1", res.Database)
		require.False(t, res.Settings.Autoload.GetValue())

	})

	t.Run("attempt to delete an open database should fail", func(t *testing.T) {
		_, err = s.DeleteDatabase(ctx, &schema.DeleteDatabaseRequest{Database: "db1"})
		require.ErrorIs(t, err, database.ErrCannotDeleteAnOpenDatabase)
	})

	t.Run("attempt to load an already loaded database should fail", func(t *testing.T) {
		_, err = s.LoadDatabase(ctx, &schema.LoadDatabaseRequest{Database: "db1"})
		require.ErrorIs(t, err, ErrDatabaseAlreadyLoaded)
	})

	t.Run("attempt to unload a loaded database should succeed", func(t *testing.T) {
		_, err = s.UnloadDatabase(ctx, &schema.UnloadDatabaseRequest{Database: "db1"})
		require.NoError(t, err)
	})

	t.Run("attempt to load an unloaded database should succeed", func(t *testing.T) {
		_, err = s.LoadDatabase(ctx, &schema.LoadDatabaseRequest{Database: "db1"})
		require.NoError(t, err)
	})

	t.Run("attempt to delete an unloaded database should succeed", func(t *testing.T) {
		_, err = s.UnloadDatabase(ctx, &schema.UnloadDatabaseRequest{Database: "db1"})
		require.NoError(t, err)

		_, err = s.DeleteDatabase(ctx, &schema.DeleteDatabaseRequest{Database: "db1"})
		require.NoError(t, err)
	})

	t.Run("attempt to load a deleted database should fail", func(t *testing.T) {
		_, err = s.LoadDatabase(ctx, &schema.LoadDatabaseRequest{Database: "db1"})
		require.ErrorIs(t, err, database.ErrDatabaseNotExists)
	})

	_, err = s.CloseSession(ctx, &emptypb.Empty{})
	require.NoError(t, err)
}

func TestServerDatabaseRuntimeEdgeCases(t *testing.T) {
	dir := t.TempDir()

	opts := DefaultOptions().WithDir(dir)

	s := DefaultServer()

	s.WithOptions(opts)

	s.Initialize()

	ctx := context.Background()

	resp, err := s.OpenSession(ctx, &schema.OpenSessionRequest{
		Username:     []byte(auth.SysAdminUsername),
		Password:     []byte(auth.SysAdminPassword),
		DatabaseName: DefaultDBName,
	})
	require.NoError(t, err)

	ctx = metadata.NewIncomingContext(context.Background(), metadata.New(map[string]string{"sessionid": resp.GetSessionID()}))

	for i, c := range []struct {
		req *schema.LoadDatabaseRequest
		err error
	}{
		{nil, ErrIllegalArguments},
		{&schema.LoadDatabaseRequest{Database: s.Options.systemAdminDBName}, ErrReservedDatabase},
		{&schema.LoadDatabaseRequest{Database: s.Options.defaultDBName}, ErrReservedDatabase},
		{&schema.LoadDatabaseRequest{Database: "unexistent_db"}, database.ErrDatabaseNotExists},
	} {
		t.Run(fmt.Sprintf("loadDatabaseCase%d", i), func(t *testing.T) {
			res, err := s.LoadDatabase(ctx, c.req)
			if c.err == nil {
				require.NoError(t, err)
			} else {
				require.ErrorIs(t, err, c.err)
			}
			require.Nil(t, res)
		})
	}

	for i, c := range []struct {
		req *schema.UpdateDatabaseRequest
		err error
	}{
		{nil, ErrIllegalArguments},
		{&schema.UpdateDatabaseRequest{Database: s.Options.systemAdminDBName}, ErrReservedDatabase},
		{&schema.UpdateDatabaseRequest{Database: s.Options.defaultDBName}, ErrReservedDatabase},
		{&schema.UpdateDatabaseRequest{Database: "unexistent_db"}, database.ErrDatabaseNotExists},
	} {
		t.Run(fmt.Sprintf("updateDatabaseCase%d", i), func(t *testing.T) {
			res, err := s.UpdateDatabaseV2(ctx, c.req)
			if c.err == nil {
				require.NoError(t, err)
			} else {
				require.ErrorIs(t, err, c.err)
			}
			require.Nil(t, res)
		})
	}

	for i, c := range []struct {
		req *schema.UnloadDatabaseRequest
		err error
	}{
		{nil, ErrIllegalArguments},
		{&schema.UnloadDatabaseRequest{Database: s.Options.systemAdminDBName}, ErrReservedDatabase},
		{&schema.UnloadDatabaseRequest{Database: s.Options.defaultDBName}, ErrReservedDatabase},
		{&schema.UnloadDatabaseRequest{Database: "unexistent_db"}, database.ErrDatabaseNotExists},
	} {
		t.Run(fmt.Sprintf("unloadDatabaseCase%d", i), func(t *testing.T) {
			res, err := s.UnloadDatabase(ctx, c.req)
			if c.err == nil {
				require.NoError(t, err)
			} else {
				require.ErrorIs(t, err, c.err)
			}
			require.Nil(t, res)
		})
	}

	for i, c := range []struct {
		req *schema.DeleteDatabaseRequest
		err error
	}{
		{nil, ErrIllegalArguments},
		{&schema.DeleteDatabaseRequest{Database: s.Options.systemAdminDBName}, ErrReservedDatabase},
		{&schema.DeleteDatabaseRequest{Database: s.Options.defaultDBName}, ErrReservedDatabase},
		{&schema.DeleteDatabaseRequest{Database: "unexistent_db"}, database.ErrDatabaseNotExists},
	} {
		t.Run(fmt.Sprintf("deleteDatabaseCase%d", i), func(t *testing.T) {
			res, err := s.DeleteDatabase(ctx, c.req)
			if c.err == nil {
				require.NoError(t, err)
			} else {
				require.ErrorIs(t, err, c.err)
			}
			require.Nil(t, res)
		})
	}

	_, err = s.CloseSession(ctx, &emptypb.Empty{})
	require.NoError(t, err)
}

// TestDatabaseListV2WithUnloadedDatabase regresses issue #1997: an unloaded
// (closed) database used to make DatabaseListV2 abort with "already closed"
// because CurrentState was called unconditionally. The listing must now
// include the unloaded entry with Loaded=false and not error out.
func TestDatabaseListV2WithUnloadedDatabase(t *testing.T) {
	dir := t.TempDir()

	s := DefaultServer()
	s.WithOptions(DefaultOptions().WithDir(dir))
	s.Initialize()

	ctx := context.Background()

	resp, err := s.OpenSession(ctx, &schema.OpenSessionRequest{
		Username:     []byte(auth.SysAdminUsername),
		Password:     []byte(auth.SysAdminPassword),
		DatabaseName: DefaultDBName,
	})
	require.NoError(t, err)
	ctx = metadata.NewIncomingContext(context.Background(), metadata.New(map[string]string{"sessionid": resp.GetSessionID()}))

	_, err = s.CreateDatabaseV2(ctx, &schema.CreateDatabaseRequest{Name: "loaded_db"})
	require.NoError(t, err)

	_, err = s.CreateDatabaseV2(ctx, &schema.CreateDatabaseRequest{Name: "unloaded_db"})
	require.NoError(t, err)

	_, err = s.UnloadDatabase(ctx, &schema.UnloadDatabaseRequest{Database: "unloaded_db"})
	require.NoError(t, err)

	listResp, err := s.DatabaseListV2(ctx, &schema.DatabaseListRequestV2{})
	require.NoError(t, err)
	require.NotNil(t, listResp)

	got := map[string]bool{}
	for _, db := range listResp.Databases {
		got[db.Name] = db.Loaded
	}

	loaded, ok := got["loaded_db"]
	require.True(t, ok, "loaded_db should be present in the listing")
	require.True(t, loaded)

	loaded, ok = got["unloaded_db"]
	require.True(t, ok, "unloaded_db should be present in the listing even though it is closed")
	require.False(t, loaded)

	v1Resp, err := s.DatabaseList(ctx, &emptypb.Empty{})
	require.NoError(t, err)
	v1Names := map[string]bool{}
	for _, db := range v1Resp.Databases {
		v1Names[db.DatabaseName] = true
	}
	require.True(t, v1Names["loaded_db"])
	require.True(t, v1Names["unloaded_db"])

	_, err = s.CloseSession(ctx, &emptypb.Empty{})
	require.NoError(t, err)
}

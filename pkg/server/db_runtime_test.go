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
	"context"
	"os"
	"testing"

	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/codenotary/immudb/pkg/auth"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/types/known/emptypb"
)

func TestServerDatabaseRuntime(t *testing.T) {
	ctx := context.Background()

	serverOptions := DefaultOptions().
		WithMetricsServer(false).
		WithAdminPassword(auth.SysAdminPassword)

	s := DefaultServer().WithOptions(serverOptions).(*ImmuServer)
	defer os.RemoveAll(s.Options.Dir)

	s.Initialize()

	resp, err := s.OpenSession(ctx, &schema.OpenSessionRequest{
		Username:     []byte(auth.SysAdminUsername),
		Password:     []byte(auth.SysAdminPassword),
		DatabaseName: DefaultDBName,
	})
	require.NoError(t, err)

	ctx = metadata.NewIncomingContext(context.TODO(), metadata.New(map[string]string{"sessionid": resp.GetSessionID()}))

	t.Run("reserved databases can not be updated", func(t *testing.T) {
		_, err = s.UpdateDatabaseV2(ctx, &schema.UpdateDatabaseRequest{
			Database: SystemDBName,
			Settings: &schema.DatabaseNullableSettings{
				Autoload: &schema.NullableBool{Value: false},
			},
		})
		require.Error(t, err)
		require.Contains(t, err.Error(), "database is reserved")

		_, err = s.UpdateDatabaseV2(ctx, &schema.UpdateDatabaseRequest{
			Database: DefaultDBName,
			Settings: &schema.DatabaseNullableSettings{
				Autoload: &schema.NullableBool{Value: false},
			},
		})
		require.Error(t, err)
		require.Contains(t, err.Error(), "database is reserved")
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

	_, err = s.CloseSession(ctx, &emptypb.Empty{})
	require.NoError(t, err)
}

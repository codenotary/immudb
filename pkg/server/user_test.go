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

	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/codenotary/immudb/pkg/auth"
	"github.com/codenotary/immudb/pkg/database"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/types/known/emptypb"
)

func TestServerLogin(t *testing.T) {
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
	resp, err := s.Login(context.Background(), r)
	require.NoError(t, err)
	if len(resp.Token) == 0 {
		t.Fatalf("login token is empty")
	}
	if len(resp.Warning) == 0 {
		t.Fatalf("default immudb password missing warning")
	}
}

func TestServerLogout(t *testing.T) {
	dir := t.TempDir()

	serverOptions := DefaultOptions().
		WithDir(dir).
		WithMetricsServer(false).
		WithAdminPassword(auth.SysAdminPassword)

	s := DefaultServer().WithOptions(serverOptions).(*ImmuServer)

	s.Initialize()

	_, err := s.Logout(context.Background(), &emptypb.Empty{})
	if err == nil || err.Error() != ErrNotLoggedIn.Message() {
		t.Fatalf("Logout expected error, got %v", err)
	}

	r := &schema.LoginRequest{
		User:     []byte(auth.SysAdminUsername),
		Password: []byte(auth.SysAdminPassword),
	}
	ctx := context.Background()
	l, err := s.Login(ctx, r)
	require.NoError(t, err)
	m := make(map[string]string)
	m["Authorization"] = "Bearer " + string(l.Token)
	ctx = metadata.NewIncomingContext(ctx, metadata.New(m))
	_, err = s.Logout(ctx, &emptypb.Empty{})
	require.NoError(t, err)
}

func TestServerLoginLogoutWithAuthDisabled(t *testing.T) {
	dir := t.TempDir()

	serverOptions := DefaultOptions().
		WithDir(dir).
		WithMetricsServer(false).
		WithAuth(false)

	s := DefaultServer().WithOptions(serverOptions).(*ImmuServer)

	s.Initialize()

	_, err := s.Logout(context.Background(), &emptypb.Empty{})
	require.NotNil(t, err)
	require.Equal(t, ErrAuthDisabled, err.Error())
}

func TestServerListUsersAdmin(t *testing.T) {
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

	newdb := &schema.DatabaseSettings{
		DatabaseName: testDatabase,
	}
	_, err = s.CreateDatabaseWith(ctx, newdb)
	require.NoError(t, err)

	err = s.CloseDatabases()
	require.NoError(t, err)

	s.dbList = database.NewDatabaseList()
	s.sysDB = nil

	err = s.loadSystemDatabase(s.Options.Dir, nil, auth.SysAdminPassword, false)
	require.NoError(t, err)

	err = s.loadDefaultDatabase(s.Options.Dir, nil)
	require.NoError(t, err)

	err = s.loadUserDatabases(s.Options.Dir, nil)
	require.NoError(t, err)

	users1, err := s.ListUsers(ctx, &emptypb.Empty{})
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(users1.Users), 1)

	newUser := &schema.CreateUserRequest{
		User:       testUsername,
		Password:   testPassword,
		Database:   testDatabase,
		Permission: auth.PermissionAdmin,
	}
	_, err = s.CreateUser(ctx, newUser)
	require.NoError(t, err)
	s.multidbmode = true
	lr, err = s.Login(ctx, &schema.LoginRequest{User: testUsername, Password: testPassword})
	require.NoError(t, err)
	md = metadata.Pairs("authorization", lr.Token)
	ctx = metadata.NewIncomingContext(context.Background(), md)
	ur, err := s.UseDatabase(ctx, &schema.Database{
		DatabaseName: testDatabase,
	})
	require.NoError(t, err)
	md = metadata.Pairs("authorization", ur.Token)
	ctx = metadata.NewIncomingContext(context.Background(), md)

	users, err := s.ListUsers(ctx, &emptypb.Empty{})
	require.NoError(t, err)
	if len(users.Users) < 1 {
		t.Fatalf("List users, expected >1 got %v", len(users.Users))
	}

	lr, err = s.Login(ctx, &schema.LoginRequest{User: []byte(auth.SysAdminUsername), Password: []byte(auth.SysAdminPassword)})
	require.NoError(t, err)
	md = metadata.Pairs("authorization", lr.Token)
	ctx = metadata.NewIncomingContext(context.Background(), md)

	require.NoError(t, err)
	users, err = s.ListUsers(ctx, &emptypb.Empty{})
	require.NoError(t, err)
	if len(users.Users) < 1 {
		t.Fatalf("List users, expected >1 got %v", len(users.Users))
	}
	require.Equal(t, len(users1.Users)+1, len(users.Users))

	newUser = &schema.CreateUserRequest{
		User:       []byte("rwuser"),
		Password:   []byte("rwuserPas@1"),
		Database:   testDatabase,
		Permission: auth.PermissionRW,
	}
	_, err = s.CreateUser(ctx, newUser)
	require.NoError(t, err)
	s.multidbmode = true

	lr, err = s.Login(ctx, &schema.LoginRequest{User: []byte("rwuser"), Password: []byte("rwuserPas@1")})
	require.NoError(t, err)
	md = metadata.Pairs("authorization", lr.Token)
	ctx = metadata.NewIncomingContext(context.Background(), md)

	ur, err = s.UseDatabase(ctx, &schema.Database{
		DatabaseName: testDatabase,
	})
	require.NoError(t, err)
	md = metadata.Pairs("authorization", ur.Token)
	ctx = metadata.NewIncomingContext(context.Background(), md)

	users, err = s.ListUsers(ctx, &emptypb.Empty{})
	require.NoError(t, err)
	if len(users.Users) < 1 {
		t.Fatalf("List users, expected >1 got %v", len(users.Users))
	}
}

func TestServerUsermanagement(t *testing.T) {
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

	newdb := &schema.DatabaseSettings{
		DatabaseName: testDatabase,
	}
	_, err = s.CreateDatabaseWith(ctx, newdb)
	require.NoError(t, err)

	testServerCreateUser(ctx, s, t)
	testServerListDatabases(ctx, s, t)
	testServerUseDatabase(ctx, s, t)
	testServerChangePermission(ctx, s, t)
	testServerDeactivateUser(ctx, s, t)
	testServerSetActiveUser(ctx, s, t)
	testServerChangePassword(ctx, s, t)
	testServerListUsers(ctx, s, t)
}

func testServerCreateUser(ctx context.Context, s *ImmuServer, t *testing.T) {
	newUser := &schema.CreateUserRequest{
		User:       testUsername,
		Password:   testPassword,
		Database:   testDatabase,
		Permission: auth.PermissionAdmin,
	}
	_, err := s.CreateUser(ctx, newUser)
	require.NoError(t, err)

	if !s.mandatoryAuth() {
		t.Fatalf("mandatoryAuth expected true")
	}
}

func testServerListUsers(ctx context.Context, s *ImmuServer, t *testing.T) {
	users, err := s.ListUsers(ctx, &emptypb.Empty{})
	require.NoError(t, err)
	if len(users.Users) < 1 {
		t.Fatalf("List users, expected >1 got %v", len(users.Users))
	}
}

func testServerListDatabases(ctx context.Context, s *ImmuServer, t *testing.T) {
	dbs, err := s.DatabaseList(ctx, &emptypb.Empty{})
	require.NoError(t, err)
	if len(dbs.Databases) < 1 {
		t.Fatalf("List databases, expected >1 got %v", len(dbs.Databases))
	}
}

func testServerUseDatabase(ctx context.Context, s *ImmuServer, t *testing.T) {
	dbs, err := s.UseDatabase(ctx, &schema.Database{
		DatabaseName: testDatabase,
	})
	require.NoError(t, err)
	if len(dbs.Token) == 0 {
		t.Fatalf("Expected token, got %v", dbs.Token)
	}
}

func testServerChangePermission(ctx context.Context, s *ImmuServer, t *testing.T) {
	_, err := s.ChangePermission(ctx, &schema.ChangePermissionRequest{
		Action:     schema.PermissionAction_GRANT,
		Database:   testDatabase,
		Permission: auth.PermissionR,
		Username:   string(testUsername),
	})

	require.NoError(t, err)
}

func testServerDeactivateUser(ctx context.Context, s *ImmuServer, t *testing.T) {
	_, err := s.SetActiveUser(ctx, &schema.SetActiveUserRequest{
		Active:   false,
		Username: string(testUsername),
	})
	require.NoError(t, err)
}

func testServerSetActiveUser(ctx context.Context, s *ImmuServer, t *testing.T) {
	_, err := s.SetActiveUser(ctx, &schema.SetActiveUserRequest{
		Active:   true,
		Username: string(testUsername),
	})
	require.NoError(t, err)
}

func testServerChangePassword(ctx context.Context, s *ImmuServer, t *testing.T) {
	_, err := s.ChangePassword(ctx, &schema.ChangePasswordRequest{
		NewPassword: testPassword,
		OldPassword: testPassword,
		User:        testUsername,
	})
	require.NoError(t, err)
}

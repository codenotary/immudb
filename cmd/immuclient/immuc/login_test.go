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

package immuc_test

import (
	"strings"
	"testing"

	. "github.com/codenotary/immudb/cmd/immuclient/immuc"
	"github.com/codenotary/immudb/cmd/immuclient/immuclienttest"
	test "github.com/codenotary/immudb/cmd/immuclient/immuclienttest"
	"github.com/codenotary/immudb/pkg/client/tokenservice"
	"github.com/codenotary/immudb/pkg/server"
	"github.com/codenotary/immudb/pkg/server/servertest"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

func TestLogin(t *testing.T) {
	options := server.DefaultOptions().WithDir(t.TempDir())
	bs := servertest.NewBufconnServer(options)

	bs.Start()
	defer bs.Stop()

	opts := OptionsFromEnv()
	opts.GetImmudbClientOptions().
		WithDialOptions([]grpc.DialOption{
			grpc.WithContextDialer(bs.Dialer), grpc.WithInsecure(),
		}).
		WithPasswordReader(&immuclienttest.PasswordReader{
			Pass: []string{"immudb"},
		}).
		WithDir(t.TempDir())

	imc, err := Init(opts)
	require.NoError(t, err)
	err = imc.Connect([]string{""})
	require.NoError(t, err)
	imc.WithFileTokenService(tokenservice.NewInmemoryTokenService())

	msg, err := imc.Login([]string{"immudb"})
	require.NoError(t, err)
	if !strings.Contains(msg, "Successfully logged in") {
		t.Fatal("Login error")
	}
}

func TestLogout(t *testing.T) {
	options := server.DefaultOptions().WithDir(t.TempDir())
	bs := servertest.NewBufconnServer(options)

	bs.Start()
	defer bs.Stop()

	opts := OptionsFromEnv()
	opts.GetImmudbClientOptions().
		WithDialOptions([]grpc.DialOption{
			grpc.WithContextDialer(bs.Dialer), grpc.WithInsecure(),
		}).
		WithPasswordReader(&immuclienttest.PasswordReader{
			Pass: []string{"immudb"},
		}).
		WithDir(t.TempDir())

	imc, err := Init(opts)
	require.NoError(t, err)
	err = imc.Connect([]string{""})
	require.NoError(t, err)
	imc.WithFileTokenService(tokenservice.NewInmemoryTokenService())

	_, err = imc.Logout([]string{""})
	require.NoError(t, err)
}

func TestUserList(t *testing.T) {
	ic := setupTest(t)

	_, err := ic.Imc.UserList([]string{""})
	require.NoError(t, err, "Userlist fail")
}

func TestUserCreate(t *testing.T) {
	icMain := setupTest(t)

	var userCreateTests = []struct {
		name     string
		args     []string
		password string
		expected string
		test     func(*testing.T, string, []string, string)
	}{
		{
			"Create user",
			[]string{"myuser", "readwrite", "defaultdb"},
			"MyUser@9",
			"Created user",
			func(t *testing.T, password string, args []string, exp string) {
				ic := test.NewClientTest(&test.PasswordReader{
					Pass: []string{password, password},
				}, icMain.Ts, icMain.Options.GetImmudbClientOptions())
				ic.Connect(icMain.Dialer)

				msg, err := ic.Imc.UserCreate(args)
				require.NoError(t, err, "TestUserCreate fail")
				if !strings.Contains(msg, exp) {
					t.Fatalf("TestUserCreate failed to create user: %s", msg)
				}
			},
		},
		{
			"Create user read",
			[]string{"myuserRead", "read", "defaultdb"},
			"MyUser@9",
			"Created user",
			func(t *testing.T, password string, args []string, exp string) {
				ic := test.NewClientTest(&test.PasswordReader{
					Pass: []string{password, password},
				}, icMain.Ts, icMain.Options.GetImmudbClientOptions())
				ic.Connect(icMain.Dialer)

				msg, err := ic.Imc.UserCreate(args)
				require.NoError(t, err, "TestUserCreate fail")
				if !strings.Contains(msg, exp) {
					t.Fatalf("TestUserCreate failed to create user: %s", msg)
				}
			},
		},
		{
			"Create user admin",
			[]string{"myuseradmin", "admin", "defaultdb"},
			"MyUser@9",
			"Created user",
			func(t *testing.T, password string, args []string, exp string) {
				ic := test.NewClientTest(&test.PasswordReader{
					Pass: []string{password, password},
				}, icMain.Ts, icMain.Options.GetImmudbClientOptions())
				ic.Connect(icMain.Dialer)

				msg, err := ic.Imc.UserCreate(args)
				require.NoError(t, err, "TestUserCreate fail")
				if !strings.Contains(msg, exp) {
					t.Fatalf("TestUserCreate failed to create user: %s", msg)
				}
			},
		},
		{
			"Create user wrong permission",
			[]string{"myuserguard", "guard", "defaultdb"},
			"MyUser@9",
			"permission value not recognized.",
			func(t *testing.T, password string, args []string, exp string) {
				ic := test.NewClientTest(&test.PasswordReader{
					Pass: []string{password, password},
				}, icMain.Ts, icMain.Options.GetImmudbClientOptions())
				ic.Connect(icMain.Dialer)

				msg, err := ic.Imc.UserCreate(args)
				require.NoError(t, err, "TestUserCreate fail")
				if !strings.Contains(msg, exp) {
					t.Fatalf("TestUserCreate failed to create user: %s", msg)
				}
			},
		},
		{
			"Create duplicate user",
			[]string{"myuser", "readwrite", "defaultdb"},
			"MyUser@9",
			"user already exists",
			func(t *testing.T, password string, args []string, exp string) {
				ic := test.NewClientTest(&test.PasswordReader{
					Pass: []string{password, password},
				}, icMain.Ts, icMain.Options.GetImmudbClientOptions())
				ic.Connect(icMain.Dialer)

				msg, err := ic.Imc.UserCreate(args)
				if err == nil {
					t.Fatal("TestUserCreate fail", err)
				}
				if !strings.Contains(err.Error(), exp) {
					t.Fatalf("TestUserCreate failed to create user: %s", err)
				}
				if msg != "" {
					t.Fatalf("TestUserCreate %s", msg)
				}
			},
		},
	}
	for _, tt := range userCreateTests {
		t.Run(tt.name, func(t *testing.T) {
			tt.test(t, tt.password, tt.args, tt.expected)
		})
	}
}

func TestUserChangePassword(t *testing.T) {
	ic := setupTest(t)

	var userCreateTests = []struct {
		name     string
		args     []string
		password string
		expected string
		test     func(*testing.T, string, []string, string)
	}{
		{
			"Change user password",
			[]string{"immudb"},
			"MyUser@9",
			"Password of immudb was successfully changed",
			func(t *testing.T, password string, args []string, exp string) {
				ic.Pr = &test.PasswordReader{
					Pass: []string{"immudb", password, password},
				}
				ic.Connect(ic.Dialer)
				msg, err := ic.Imc.ChangeUserPassword(args)
				require.NoError(t, err, "TestUserChangePassword fail")
				if !strings.Contains(msg, exp) {
					t.Fatalf("TestUserChangePassword failed to change password: %s", msg)
				}
			},
		},
		{
			"Change user password wrong old password",
			[]string{"immudb"},
			"MyUser@9",
			"old password is incorrect",
			func(t *testing.T, password string, args []string, exp string) {
				ic.Pr = &test.PasswordReader{
					Pass: []string{password},
				}
				ic.Connect(ic.Dialer)
				ic.Login("immudb")

				ic.Pr = &test.PasswordReader{
					Pass: []string{"pass", password, password},
				}
				ic.Connect(ic.Dialer)
				msg, err := ic.Imc.ChangeUserPassword(args)
				if err == nil {
					t.Fatal("TestUserChangePassword fail", err)
				}
				if !strings.Contains(err.Error(), exp) {
					t.Fatalf("TestUserChangePassword failed to change password: %s", msg)
				}
			},
		},
	}
	for _, tt := range userCreateTests {
		t.Run(tt.name, func(t *testing.T) {
			tt.test(t, tt.password, tt.args, tt.expected)
		})
	}
}

func TestUserSetActive(t *testing.T) {
	ic := setupTest(t)

	ic.Pr = &test.PasswordReader{
		Pass: []string{"MyUser@9", "MyUser@9"},
	}
	ic.Connect(ic.Dialer)

	_, err := ic.Imc.UserCreate([]string{"myuser", "readwrite", "defaultdb"})
	require.NoError(t, err, "TestUserCreate fail")
	var userCreateTests = []struct {
		name     string
		args     []string
		password string
		expected string
		test     func(*testing.T, string, []string, string)
	}{
		{
			"SetActiveUser",
			[]string{"myuser"},
			"",
			"user status changed successfully",
			func(t *testing.T, password string, args []string, exp string) {
				msg, err := ic.Imc.SetActiveUser(args, true)
				require.NoError(t, err, "SetActiveUser fail")
				if !strings.Contains(msg, exp) {
					t.Fatalf("SetActiveUser failed to change status: %s", msg)
				}
			},
		},
		{
			"Deactivate user",
			[]string{"myuser"},
			"",
			"user status changed successfully",
			func(t *testing.T, password string, args []string, exp string) {
				msg, err := ic.Imc.SetActiveUser(args, false)
				require.NoError(t, err, "Deactivate fail")
				if !strings.Contains(msg, exp) {
					t.Fatalf("Deactivate failed to change status: %s", msg)
				}
			},
		},
	}
	for _, tt := range userCreateTests {
		t.Run(tt.name, func(t *testing.T) {
			tt.test(t, tt.password, tt.args, tt.expected)
		})
	}
}

func TestSetUserPermission(t *testing.T) {
	ic := setupTest(t)

	ic.Pr = &test.PasswordReader{
		Pass: []string{"MyUser@9", "MyUser@9"},
	}
	ic.Connect(ic.Dialer)
	_, err := ic.Imc.UserCreate([]string{"myuser", "readwrite", "defaultdb"})
	require.NoError(t, err, "TestUserCreate fail")
	var userCreateTests = []struct {
		name     string
		args     []string
		password string
		expected string
		test     func(*testing.T, string, []string, string)
	}{
		{
			"SetUserPermission user",
			[]string{"grant", "myuser", "admin", "defaultdb"},
			"MyUser@9",
			"permission changed successfully",
			func(t *testing.T, password string, args []string, exp string) {
				msg, err := ic.Imc.SetUserPermission(args)
				require.NoError(t, err, "SetUserPermission fail")
				if !strings.Contains(msg, exp) {
					t.Fatalf("SetUserPermission failed to set user permission: %s", msg)
				}
			},
		},
		{
			"SetUserPermission user",
			[]string{"revoke", "myuser", "admin", "defaultdb"},
			"MyUser@9",
			"permission changed successfully",
			func(t *testing.T, password string, args []string, exp string) {
				msg, err := ic.Imc.SetUserPermission(args)
				require.NoError(t, err, "SetUserPermission fail")
				if !strings.Contains(msg, exp) {
					t.Fatalf("SetUserPermission failed to set user permission: %s", msg)
				}
			},
		},
		{
			"SetUserPermission user",
			[]string{"grant", "myuser", "readwrite", "defaultdb"},
			"MyUser@9",
			"permission changed successfully",
			func(t *testing.T, password string, args []string, exp string) {
				msg, err := ic.Imc.SetUserPermission(args)
				require.NoError(t, err, "SetUserPermission fail")
				if !strings.Contains(msg, exp) {
					t.Fatalf("SetUserPermission failed to set user permission: %s", msg)
				}
			},
		},
		{
			"SetUserPermission user",
			[]string{"grant", "myuser", "read", "defaultdb"},
			"MyUser@9",
			"permission changed successfully",
			func(t *testing.T, password string, args []string, exp string) {
				msg, err := ic.Imc.SetUserPermission(args)
				require.NoError(t, err, "SetUserPermission fail")
				if !strings.Contains(msg, exp) {
					t.Fatalf("SetUserPermission failed to set user permission: %s", msg)
				}
			},
		},
	}
	for _, tt := range userCreateTests {
		t.Run(tt.name, func(t *testing.T) {
			tt.test(t, tt.password, tt.args, tt.expected)
		})
	}
}

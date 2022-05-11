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

package immuc_test

import (
	"os"
	"strings"
	"testing"

	"github.com/codenotary/immudb/cmd/cmdtest"
	"github.com/codenotary/immudb/pkg/client/tokenservice"

	. "github.com/codenotary/immudb/cmd/immuclient/immuc"
	"github.com/codenotary/immudb/cmd/immuclient/immuclienttest"
	test "github.com/codenotary/immudb/cmd/immuclient/immuclienttest"
	"github.com/codenotary/immudb/pkg/client"
	"github.com/codenotary/immudb/pkg/server"
	"github.com/codenotary/immudb/pkg/server/servertest"
	"github.com/spf13/viper"
	"google.golang.org/grpc"
)

func TestLogin(t *testing.T) {
	viper.Set("tokenfile", "token")
	options := server.DefaultOptions().WithAuth(true)
	bs := servertest.NewBufconnServer(options)

	bs.Start()
	defer bs.Stop()

	defer os.RemoveAll(options.Dir)
	defer os.Remove(".state-")

	opts := OptionsFromEnv()
	opts.GetImmudbClientOptions().
		WithDialOptions([]grpc.DialOption{
			grpc.WithContextDialer(bs.Dialer), grpc.WithInsecure(),
		}).
		WithPasswordReader(&immuclienttest.PasswordReader{
			Pass: []string{"immudb"},
		})

	imc, err := Init(opts)
	if err != nil {
		t.Fatal(err)
	}
	err = imc.Connect([]string{""})
	if err != nil {
		t.Fatal(err)
	}

	msg, err := imc.Login([]string{"immudb"})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(msg, "Successfully logged in") {
		t.Fatal("Login error")
	}
}
func TestLogout(t *testing.T) {
	viper.Set("tokenfile", client.DefaultOptions().TokenFileName)
	options := server.DefaultOptions().WithAuth(true)
	bs := servertest.NewBufconnServer(options)

	bs.Start()
	defer bs.Stop()

	defer os.RemoveAll(options.Dir)
	defer os.Remove(".state-")

	opts := OptionsFromEnv()
	opts.GetImmudbClientOptions().
		WithDialOptions([]grpc.DialOption{
			grpc.WithContextDialer(bs.Dialer), grpc.WithInsecure(),
		}).
		WithPasswordReader(&immuclienttest.PasswordReader{
			Pass: []string{"immudb"},
		})

	imc, err := Init(opts)
	if err != nil {
		t.Fatal(err)
	}
	err = imc.Connect([]string{""})
	if err != nil {
		t.Fatal(err)
	}
	_, err = imc.Logout([]string{""})
	if err != nil {
		t.Fatal(err)
	}
}

func TestUserList(t *testing.T) {
	options := server.DefaultOptions().WithAuth(true)
	bs := servertest.NewBufconnServer(options)

	bs.Start()
	defer bs.Stop()

	defer os.RemoveAll(options.Dir)
	defer os.Remove(".state-")
	tkf := cmdtest.RandString()
	ts := tokenservice.NewFileTokenService().WithTokenFileName(tkf)
	ic := test.NewClientTest(&test.PasswordReader{
		Pass: []string{"immudb"},
	}, ts)
	ic.Connect(bs.Dialer)
	ic.Login("immudb")

	_, err := ic.Imc.UserList([]string{""})
	if err != nil {
		t.Fatal("Userlist fail", err)
	}
}
func TestUserCreate(t *testing.T) {
	options := server.DefaultOptions().WithAuth(true)
	bs := servertest.NewBufconnServer(options)

	bs.Start()
	defer bs.Stop()

	defer os.RemoveAll(options.Dir)
	defer os.Remove(".state-")
	tkf := cmdtest.RandString()
	ts := tokenservice.NewFileTokenService().WithTokenFileName(tkf)
	ic := test.NewClientTest(&test.PasswordReader{
		Pass: []string{"immudb"},
	}, ts)
	ic.Connect(bs.Dialer)
	ic.Login("immudb")

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
				}, ic.Ts)
				ic.Connect(bs.Dialer)

				msg, err := ic.Imc.UserCreate(args)
				if err != nil {
					t.Fatal("TestUserCreate fail", err)
				}
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
				}, ic.Ts)
				ic.Connect(bs.Dialer)

				msg, err := ic.Imc.UserCreate(args)
				if err != nil {
					t.Fatal("TestUserCreate fail", err)
				}
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
				}, ic.Ts)
				ic.Connect(bs.Dialer)

				msg, err := ic.Imc.UserCreate(args)
				if err != nil {
					t.Fatal("TestUserCreate fail", err)
				}
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
				}, ic.Ts)
				ic.Connect(bs.Dialer)

				msg, err := ic.Imc.UserCreate(args)
				if err != nil {
					t.Fatal("TestUserCreate fail", err)
				}
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
				}, ic.Ts)
				ic.Connect(bs.Dialer)

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
	options := server.DefaultOptions().WithAuth(true)
	bs := servertest.NewBufconnServer(options)

	bs.Start()
	defer bs.Stop()

	defer os.RemoveAll(options.Dir)
	defer os.Remove(".state-")

	tkf := cmdtest.RandString()
	ts := tokenservice.NewFileTokenService().WithTokenFileName(tkf)
	ic := test.NewClientTest(&test.PasswordReader{
		Pass: []string{"immudb"},
	}, ts)
	ic.
		Connect(bs.Dialer)
	ic.Login("immudb")

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
				ic.Connect(bs.Dialer)
				msg, err := ic.Imc.ChangeUserPassword(args)
				if err != nil {
					t.Fatal("TestUserChangePassword fail", err)
				}
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
				ic.Connect(bs.Dialer)
				ic.Login("immudb")

				ic.Pr = &test.PasswordReader{
					Pass: []string{"pass", password, password},
				}
				ic.Connect(bs.Dialer)
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
	options := server.DefaultOptions().WithAuth(true)
	bs := servertest.NewBufconnServer(options)

	bs.Start()
	defer bs.Stop()

	defer os.RemoveAll(options.Dir)
	defer os.Remove(".state-")

	tkf := cmdtest.RandString()
	ts := tokenservice.NewFileTokenService().WithTokenFileName(tkf)
	ic := test.NewClientTest(&test.PasswordReader{
		Pass: []string{"immudb"},
	}, ts)
	ic.
		Connect(bs.Dialer)
	ic.Login("immudb")

	ic.Pr = &test.PasswordReader{
		Pass: []string{"MyUser@9", "MyUser@9"},
	}
	ic.Connect(bs.Dialer)

	_, err := ic.Imc.UserCreate([]string{"myuser", "readwrite", "defaultdb"})
	if err != nil {
		t.Fatal("TestUserCreate fail", err)
	}
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
				if err != nil {
					t.Fatal("SetActiveUser fail", err)
				}
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
				if err != nil {
					t.Fatal("Deactivate fail", err)
				}
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
	options := server.DefaultOptions().WithAuth(true)
	bs := servertest.NewBufconnServer(options)

	bs.Start()
	defer bs.Stop()

	defer os.RemoveAll(options.Dir)
	defer os.Remove(".state-")

	tkf := cmdtest.RandString()
	ts := tokenservice.NewFileTokenService().WithTokenFileName(tkf)
	ic := test.NewClientTest(&test.PasswordReader{
		Pass: []string{"immudb"},
	}, ts)
	ic.Connect(bs.Dialer)
	ic.Login("immudb")

	ic.Pr = &test.PasswordReader{
		Pass: []string{"MyUser@9", "MyUser@9"},
	}
	ic.Connect(bs.Dialer)
	_, err := ic.Imc.UserCreate([]string{"myuser", "readwrite", "defaultdb"})
	if err != nil {
		t.Fatal("TestUserCreate fail", err)
	}
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
				if err != nil {
					t.Fatal("SetUserPermission fail", err)
				}
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
				if err != nil {
					t.Fatal("SetUserPermission fail", err)
				}
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
				if err != nil {
					t.Fatal("SetUserPermission fail", err)
				}
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
				if err != nil {
					t.Fatal("SetUserPermission fail", err)
				}
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

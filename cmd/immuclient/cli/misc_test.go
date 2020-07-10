/*
Copyright 2019-2020 vChain, Inc.

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

package cli

import (
	"strings"
	"testing"

	"github.com/codenotary/immudb/cmd/immuclient/immuclienttest"
	"github.com/codenotary/immudb/pkg/server"
	"github.com/codenotary/immudb/pkg/server/servertest"
)

func TestHealthCheck(t *testing.T) {
	options := server.DefaultOptions().WithAuth(true).WithInMemoryStore(true)
	bs := servertest.NewBufconnServer(options)
	bs.Start()

	imc := newClient(&immuclienttest.PasswordReader{
		Pass: []string{},
	}, bs.Dialer)
	bs.GrpcServer.Stop()

	cli := new(cli)
	cli.immucl = imc

	msg, err := cli.healthCheck([]string{})
	if err != nil {
		t.Fatal("HealthCheck fail stoped server", err)
	}
	if !strings.Contains(msg, "Error while dialing closed") {
		t.Fatal("HealthCheck fail stoped server", msg)
	}

	bs = servertest.NewBufconnServer(options)
	bs.Start()
	imc = login("immudb", "immudb", bs.Dialer)
	cli.immucl = imc
	msg, err = cli.healthCheck([]string{})
	if err != nil {
		t.Fatal("HealthCheck fail", err)
	}
	if !strings.Contains(msg, "Health check OK") {
		t.Fatal("HealthCheck fail")
	}
}

func TestHistory(t *testing.T) {
	options := server.DefaultOptions().WithAuth(true).WithInMemoryStore(true)
	bs := servertest.NewBufconnServer(options)
	bs.Start()

	imc := login("immudb", "immudb", bs.Dialer)

	cli := new(cli)
	cli.immucl = imc

	msg, err := cli.history([]string{"key"})
	if err != nil {
		t.Fatal("History fail", err)
	}
	if !strings.Contains(msg, "No item found") {
		t.Fatalf("History fail %s", msg)
	}

	msg, err = cli.set([]string{"key", "value"})
	if err != nil {
		t.Fatal("History fail", err)
	}
	msg, err = cli.history([]string{"key"})
	if err != nil {
		t.Fatal("History fail", err)
	}
	if !strings.Contains(msg, "hash") {
		t.Fatalf("History fail %s", msg)
	}
}
func TestVersion(t *testing.T) {
	options := server.DefaultOptions().WithAuth(true).WithInMemoryStore(true)
	bs := servertest.NewBufconnServer(options)
	bs.Start()

	imc := login("immudb", "immudb", bs.Dialer)

	cli := new(cli)
	cli.immucl = imc

	msg, err := cli.version([]string{"key"})
	if err != nil {
		t.Fatal("version fail", err)
	}
	if !strings.Contains(msg, "no version info available") {
		t.Fatalf("version fail %s", msg)
	}
}
func TestUserList(t *testing.T) {
	options := server.DefaultOptions().WithAuth(true).WithInMemoryStore(true)
	bs := servertest.NewBufconnServer(options)
	bs.Start()
	imc := login("immudb", "immudb", bs.Dialer)

	cli := new(cli)
	cli.immucl = imc

	_, err := cli.UserOperations([]string{"list"})
	if err != nil {
		t.Fatal("Userlist fail", err)
	}
}

func TestUserChangePassword(t *testing.T) {
	options := server.DefaultOptions().WithAuth(true).WithInMemoryStore(true)
	bs := servertest.NewBufconnServer(options)
	bs.Start()
	imc := login("immudb", "immudb", bs.Dialer)

	var userCreateTests = []struct {
		name     string
		args     []string
		password string
		expected string
		test     func(*testing.T, string, []string, string)
	}{
		{
			"Change user password",
			[]string{"changepassword", "immudb"},
			"MyUser@9",
			"Password of immudb was changed successfuly",
			func(t *testing.T, password string, args []string, exp string) {
				imc = newClient(&immuclienttest.PasswordReader{
					Pass: []string{"immudb", password, password},
				}, bs.Dialer)
				cli := new(cli)
				cli.immucl = imc
				msg, err := cli.UserOperations(args)
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
			[]string{"changepassword", "immudb"},
			"MyUser@9",
			"old password is incorrect",
			func(t *testing.T, password string, args []string, exp string) {
				imc := login("immudb", "MyUser@9", bs.Dialer)
				imc = newClient(&immuclienttest.PasswordReader{
					Pass: []string{"immudb", password, password},
				}, bs.Dialer)
				cli := new(cli)
				cli.immucl = imc
				msg, err := cli.UserOperations(args)
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
	options := server.DefaultOptions().WithAuth(true).WithInMemoryStore(true)
	bs := servertest.NewBufconnServer(options)
	bs.Start()

	imc := login("immudb", "immudb", bs.Dialer)
	cli := new(cli)
	imc = newClient(&immuclienttest.PasswordReader{
		Pass: []string{"MyUser@9", "MyUser@9"},
	}, bs.Dialer)
	_, err := imc.UserOperations([]string{"create", "myuser", "readwrite", "defaultdb"})

	imc.Logout([]string{})
	imc = login("immudb", "immudb", bs.Dialer)
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
			[]string{"activate", "myuser"},
			"",
			"User status changed successfully",
			func(t *testing.T, password string, args []string, exp string) {
				imc = newClient(&immuclienttest.PasswordReader{
					Pass: []string{"immudb", password, password},
				}, bs.Dialer)

				cli.immucl = imc
				msg, err := cli.UserOperations(args)
				if err != nil {
					t.Fatal("UserOperations activate fail", err)
				}
				if !strings.Contains(msg, exp) {
					t.Fatalf("SetActiveUser failed to change status: %s", msg)
				}
			},
		},
		{
			"Deactivate user",
			[]string{"deactivate", "myuser"},
			"",
			"User status changed successfully",
			func(t *testing.T, password string, args []string, exp string) {
				imc = newClient(&immuclienttest.PasswordReader{
					Pass: []string{"immudb", password, password},
				}, bs.Dialer)

				cli.immucl = imc
				msg, err := cli.UserOperations(args)
				if err != nil {
					t.Fatal("Deactivate fail", err)
				}
				if !strings.Contains(msg, exp) {
					t.Fatalf("SetActiveUser failed to change status: %s", msg)
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
	options := server.DefaultOptions().WithAuth(true).WithInMemoryStore(true)
	bs := servertest.NewBufconnServer(options)
	bs.Start()
	imc := login("immudb", "immudb", bs.Dialer)

	imc = newClient(&immuclienttest.PasswordReader{
		Pass: []string{"MyUser@9", "MyUser@9"},
	}, bs.Dialer)

	cli := new(cli)

	_, err := imc.UserOperations([]string{"create", "myuser", "readwrite", "defaultdb"})
	if err != nil {
		t.Fatal("TestUserCreate fail", err)
	}
	imc.Logout([]string{})
	imc = login("immudb", "immudb", bs.Dialer)

	var userCreateTests = []struct {
		name     string
		args     []string
		password string
		expected string
		test     func(*testing.T, string, []string, string)
	}{
		{
			"SetUserPermission user",
			[]string{"permission", "grant", "myuser", "admin", "defaultdb"},
			"MyUser@9",
			"Permission changed successfully",
			func(t *testing.T, password string, args []string, exp string) {
				imc = newClient(&immuclienttest.PasswordReader{
					Pass: []string{password, password},
				}, bs.Dialer)

				cli.immucl = imc
				msg, err := cli.UserOperations(args)
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
			[]string{"permission", "revoke", "myuser", "admin", "defaultdb"},
			"MyUser@9",
			"Permission changed successfully",
			func(t *testing.T, password string, args []string, exp string) {
				imc = newClient(&immuclienttest.PasswordReader{
					Pass: []string{password, password},
				}, bs.Dialer)
				cli.immucl = imc
				msg, err := cli.UserOperations(args)
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
			"Permission changed successfully",
			func(t *testing.T, password string, args []string, exp string) {
				imc = newClient(&immuclienttest.PasswordReader{
					Pass: []string{password, password},
				}, bs.Dialer)
				msg, err := imc.SetUserPermission(args)
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
			"Permission changed successfully",
			func(t *testing.T, password string, args []string, exp string) {
				imc = newClient(&immuclienttest.PasswordReader{
					Pass: []string{password, password},
				}, bs.Dialer)
				msg, err := imc.SetUserPermission(args)
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
func TestUserWrongCommand(t *testing.T) {
	options := server.DefaultOptions().WithAuth(true).WithInMemoryStore(true)
	bs := servertest.NewBufconnServer(options)
	bs.Start()
	imc := login("immudb", "immudb", bs.Dialer)

	imc = newClient(&immuclienttest.PasswordReader{
		Pass: []string{"MyUser@9", "MyUser@9"},
	}, bs.Dialer)

	cli := new(cli)

	_, err := imc.UserOperations([]string{"create", "myuser", "readwrite", "defaultdb"})
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
			[]string{"set", "grant", "myuser", "admin", "defaultdb"},
			"MyUser@9",
			"Permission changed successfully",
			func(t *testing.T, password string, args []string, exp string) {
				imc = newClient(&immuclienttest.PasswordReader{
					Pass: []string{password, password},
				}, bs.Dialer)

				cli.immucl = imc
				_, err := cli.UserOperations(args)
				if err == nil {
					t.Fatal("Wrong command fail", err)
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

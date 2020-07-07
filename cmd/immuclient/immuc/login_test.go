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

package immuc

import (
	"context"
	"log"
	"net"
	"strings"
	"testing"

	"github.com/codenotary/immudb/cmd/helper"
	c "github.com/codenotary/immudb/cmd/helper"
	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/codenotary/immudb/pkg/auth"
	"github.com/codenotary/immudb/pkg/client"
	"github.com/codenotary/immudb/pkg/server"
	"github.com/codenotary/immudb/pkg/server/servertest"
	"github.com/spf13/viper"
	"google.golang.org/grpc"
	"google.golang.org/grpc/test/bufconn"
)

const bufSize = 1024 * 1024

var lis *bufconn.Listener

var immuServer *server.ImmuServer
var cli client.ImmuClient
var username string
var plainPass string

func newServer(authRequired bool) *server.ImmuServer {
	is := server.DefaultServer()
	is = is.WithOptions(is.Options.WithAuth(authRequired).WithInMemoryStore(true))
	auth.AuthEnabled = is.Options.GetAuth()

	username, plainPass = auth.SysAdminUsername, auth.SysAdminPassword

	lis = bufconn.Listen(bufSize)
	s := grpc.NewServer(
		grpc.UnaryInterceptor(auth.ServerUnaryInterceptor),
		grpc.StreamInterceptor(auth.ServerStreamInterceptor),
	)
	schema.RegisterImmuServiceServer(s, is)
	go func() {
		if err := s.Serve(lis); err != nil {
			log.Fatal(err)
		}
	}()
	return is
}

func bufDialer(ctx context.Context, address string) (net.Conn, error) {
	return lis.Dial()
}
func newClient(pr helper.PasswordReader) Client {
	dialOptions := []grpc.DialOption{
		grpc.WithContextDialer(bufDialer), grpc.WithInsecure(),
	}
	// token, err := client.NewHomedirService().ReadFileFromUserHomeDir(client.DefaultOptions().TokenFileName)
	// if err == nil {
	// 	dialOptions = append(dialOptions, grpc.WithUnaryInterceptor(auth.ClientUnaryInterceptor(token)))
	// 	dialOptions = append(dialOptions, grpc.WithStreamInterceptor(auth.ClientStreamInterceptor(token)))
	// }

	c.DefaultPasswordReader = pr
	imc, err := Init(Options().WithDialOptions(&dialOptions))
	if err != nil {
		log.Fatal(err)
	}
	err = imc.Connect([]string{""})
	if err != nil {
		log.Fatal(err)
	}
	return imc
}
func login(username string, password string) Client {
	viper.Set("tokenfile", client.DefaultOptions().TokenFileName)
	imc := newClient(&testPasswordReader{
		pass: password,
	})
	msg, err := imc.Login([]string{username})
	if err != nil {
		log.Fatal(err)
	}
	if !strings.Contains(msg, "Successfully logged in.") {
		log.Fatal("Login error")
	}

	// imc = newClient(&testPasswordReader{
	// 	pass: password,
	// })
	return imc
}

func TestConnect(t *testing.T) {
	options := server.Options{}.WithAuth(true).WithInMemoryStore(true)
	bs := servertest.NewBufconnServer(options)
	bs.Start()

	dialOptions := []grpc.DialOption{
		grpc.WithContextDialer(bs.Dialer), grpc.WithInsecure(),
	}
	imc, err := Init(Options().WithDialOptions(&dialOptions))
	if err != nil {
		t.Fatal(err)
	}
	err = imc.Connect([]string{""})
	if err != nil {
		t.Fatal(err)
	}
}

type testPasswordReader struct {
	pass string
}

func (pr *testPasswordReader) Read(msg string) ([]byte, error) {
	return []byte(pr.pass), nil
}

func TestLogin(t *testing.T) {
	viper.Set("tokenfile", "token")
	immuServer = newServer(true)
	immuServer.Start()

	dialOptions := []grpc.DialOption{
		grpc.WithContextDialer(bufDialer), grpc.WithInsecure(),
	}
	c.DefaultPasswordReader = &testPasswordReader{
		pass: "immudb",
	}
	imc, err := Init(Options().WithDialOptions(&dialOptions))
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
	if !strings.Contains(msg, "Successfully logged in.") {
		t.Fatal("Login error")
	}
}
func TestLogout(t *testing.T) {
	viper.Set("tokenfile", client.DefaultOptions().TokenFileName)
	immuServer = newServer(true)
	immuServer.Start()

	dialOptions := []grpc.DialOption{
		grpc.WithContextDialer(bufDialer), grpc.WithInsecure(),
	}
	pr := new(testPasswordReader)
	pr.pass = "immudb"
	c.DefaultPasswordReader = pr

	imc, err := Init(Options().WithDialOptions(&dialOptions))
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
	immuServer = newServer(true)
	immuServer.Start()
	imc := login("immudb", "immudb")
	_, err := imc.UserList([]string{""})
	if err != nil {
		t.Fatal("Userlist fail", err)
	}
}
func TestUserCreate(t *testing.T) {
	immuServer = newServer(true)
	immuServer.Start()
	imc := login("immudb", "immudb")

	imc = newClient(&testPasswordReader{
		pass: "MyUser@9",
	})

	msg, err := imc.UserOperations([]string{"create", "myuser", "readwrite", "defaultdb"})
	if err != nil {
		t.Fatal("TestUserCreate fail", err)
	}
	if !strings.Contains(msg, "Created user") {
		t.Fatalf("TestUserCreate failed to create user: %s", err)
	}
}

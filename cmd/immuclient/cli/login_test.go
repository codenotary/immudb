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
	"log"
	"strings"
	"testing"

	"github.com/codenotary/immudb/cmd/helper"
	c "github.com/codenotary/immudb/cmd/helper"
	"github.com/codenotary/immudb/cmd/immuclient/immuc"
	"github.com/codenotary/immudb/pkg/client"
	"github.com/codenotary/immudb/pkg/server"
	"github.com/codenotary/immudb/pkg/server/servertest"
	"github.com/spf13/viper"
	"google.golang.org/grpc"
)

func newClient(pr helper.PasswordReader, dialer servertest.BuffDialer) immuc.Client {
	dialOptions := []grpc.DialOption{
		grpc.WithContextDialer(dialer), grpc.WithInsecure(),
	}

	c.DefaultPasswordReader = pr
	imc, err := immuc.Init(immuc.Options().WithDialOptions(&dialOptions))
	if err != nil {
		log.Fatal(err)
	}
	err = imc.Connect([]string{""})
	if err != nil {
		log.Fatal(err)
	}
	return imc
}
func login(username string, password string, dialer servertest.BuffDialer) immuc.Client {
	viper.Set("tokenfile", client.DefaultOptions().TokenFileName)
	imc := newClient(&testPasswordReader{
		pass: []string{password, password},
	}, dialer)
	msg, err := imc.Login([]string{username})
	if err != nil {
		log.Fatal(err)
	}
	if !strings.Contains(msg, "Successfully logged in.") {
		log.Fatal("Login error")
	}

	return imc
}

type testPasswordReader struct {
	pass       []string
	callNumber int
}

func (pr *testPasswordReader) Read(msg string) ([]byte, error) {
	pass := []byte(pr.pass[pr.callNumber])
	pr.callNumber++
	return pass, nil
}
func TestLogin(t *testing.T) {
	options := server.DefaultOptions().WithAuth(true).WithInMemoryStore(true)
	bs := servertest.NewBufconnServer(options)
	bs.Start()
	imc := login("immudb", "immudb", bs.Dialer)

	cli := new(cli)

	cli.immucl = imc
	msg, err := cli.login([]string{"immudb"})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(msg, "immudb user has the default password") {
		t.Fatalf("Login failed: %s", err)
	}

	msg, err = cli.logout([]string{"immudb"})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(msg, "Successfully logged out") {
		t.Fatalf("Login failed: %s", err)
	}
}

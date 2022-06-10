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

	. "github.com/codenotary/immudb/cmd/immuclient/immuc"
	"github.com/codenotary/immudb/cmd/immuclient/immuclienttest"
	"github.com/codenotary/immudb/pkg/client"
	"github.com/codenotary/immudb/pkg/server"
	"github.com/codenotary/immudb/pkg/server/servertest"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"
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
	if !strings.Contains(msg.Plain(), "Successfully logged in") {
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

func TestUseDatabase(t *testing.T) {
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
	require.NoError(t, err)

	err = imc.Connect([]string{})
	require.NoError(t, err)

	_, err = imc.Login([]string{"immudb"})
	require.NoError(t, err)

	_, err = imc.UseDatabase([]string{"defaultdb"})
	require.NoError(t, err)
}

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

package integration

import (
	"context"
	"io/ioutil"
	"os"
	"testing"

	"github.com/codenotary/immudb/pkg/client"
	"github.com/codenotary/immudb/pkg/client/errors"
	"github.com/codenotary/immudb/pkg/server"
	"github.com/codenotary/immudb/pkg/server/servertest"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

func TestGRPCError(t *testing.T) {
	os.Setenv("LOG_LEVEL", "debug")
	defer os.Unsetenv("LOG_LEVEL")

	dir, err := ioutil.TempDir("", "integration_test")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	defer os.Remove(".state-")

	options := server.DefaultOptions().
		WithDir(dir).
		WithAuth(true)
	bs := servertest.NewBufconnServer(options)

	bs.Start()
	defer bs.Stop()

	cli, _ := client.NewImmuClient(client.DefaultOptions().WithDialOptions([]grpc.DialOption{grpc.WithContextDialer(bs.Dialer), grpc.WithInsecure()}))

	_, err = cli.Login(context.TODO(), []byte(`immudb`), []byte(`wrong`))

	require.Equal(t, err.(errors.ImmuError).Error(), "invalid user name or password")
	require.Equal(t, err.(errors.ImmuError).Cause(), "crypto/bcrypt: hashedPassword is not the hash of the given password")
	require.Equal(t, err.(errors.ImmuError).Code(), errors.CodSqlserverRejectedEstablishmentOfSqlconnection)
	require.Equal(t, int32(0), err.(errors.ImmuError).RetryDelay())
	require.NotNil(t, err.(errors.ImmuError).Stack())
}

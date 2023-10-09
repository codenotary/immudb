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
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/codenotary/immudb/embedded/store"
	ic "github.com/codenotary/immudb/pkg/client"

	"github.com/codenotary/immudb/pkg/server"
	"github.com/codenotary/immudb/pkg/server/servertest"
	"github.com/codenotary/immudb/pkg/server/sessions"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/emptypb"
)

func TestSession_OpenCloseSession(t *testing.T) {
	_, client, _ := setupTestServerAndClient(t)

	err := client.OpenSession(context.Background(), []byte(`immudb`), []byte(`immudb`), "defaultdb")
	require.ErrorIs(t, err, ic.ErrSessionAlreadyOpen)

	client.Set(context.Background(), []byte("myKey"), []byte("myValue"))

	err = client.CloseSession(context.Background())
	require.NoError(t, err)

	err = client.CloseSession(context.Background())
	require.Error(t, err)

	err = client.OpenSession(context.Background(), []byte(`immudb`), []byte(`immudb`), "defaultdb")
	require.NoError(t, err)

	client.GetServiceClient().KeepAlive(context.Background(), &emptypb.Empty{})
	require.NoError(t, err)

	entry, err := client.Get(context.Background(), []byte("myKey"))
	require.NoError(t, err)
	require.NotNil(t, entry)
	require.Equal(t, []byte("myValue"), entry.Value)

	err = client.CloseSession(context.Background())
	require.NoError(t, err)
}

func TestSession_OpenCloseSessionMulti(t *testing.T) {
	sessOptions := sessions.DefaultOptions().
		WithSessionGuardCheckInterval(time.Millisecond * 100).
		WithMaxSessionInactivityTime(time.Millisecond * 2000).
		WithMaxSessionAgeTime(time.Millisecond * 4000).
		WithTimeout(time.Millisecond * 2000)

	options := server.DefaultOptions().
		WithDir(t.TempDir()).
		WithSessionOptions(sessOptions).
		WithWebServer(false).
		WithPgsqlServer(false)

	bs := servertest.NewBufconnServer(options)

	err := bs.Start()
	require.NoError(t, err)
	defer bs.Stop()

	wg := sync.WaitGroup{}
	for i := 0; i < store.DefaultMaxConcurrency; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()

			client := ic.NewClient().WithOptions(ic.
				DefaultOptions().
				WithDir(t.TempDir()).
				WithDialOptions([]grpc.DialOption{grpc.WithContextDialer(bs.Dialer), grpc.WithTransportCredentials(insecure.NewCredentials())}).
				WithHeartBeatFrequency(time.Millisecond * 100),
			)
			err := client.OpenSession(context.Background(), []byte(`immudb`), []byte(`immudb`), "defaultdb")
			require.NoError(t, err)

			min := 10
			max := 100
			time.Sleep(time.Millisecond * time.Duration(rand.Intn(max-min)+min))

			_, err = client.Set(context.Background(), []byte(fmt.Sprintf("%d", i)), []byte(fmt.Sprintf("%d", i)))
			require.NoError(t, err)

			err = client.CloseSession(context.Background())
			require.NoError(t, err)
		}(i)
	}
	wg.Wait()
	require.Equal(t, 0, bs.Server.Srv.SessManager.SessionCount())
}

func TestSession_OpenSessionNotConnected(t *testing.T) {
	client := ic.NewClient()
	err := client.CloseSession(context.Background())
	require.ErrorIs(t, ic.ErrNotConnected, err)
}

func TestSession_ExpireSessions(t *testing.T) {
	sessOptions := sessions.DefaultOptions().
		WithSessionGuardCheckInterval(time.Millisecond * 100).
		WithMaxSessionInactivityTime(time.Millisecond * 200).
		WithMaxSessionAgeTime(time.Millisecond * 900).
		WithTimeout(time.Millisecond * 100)

	options := server.DefaultOptions().
		WithDir(t.TempDir()).
		WithSessionOptions(sessOptions)

	bs := servertest.NewBufconnServer(options)

	err := bs.Start()
	require.NoError(t, err)

	defer bs.Stop()

	rand.Seed(time.Now().UnixNano())
	wg := sync.WaitGroup{}
	for i := 1; i <= 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			client := ic.NewClient().WithOptions(ic.
				DefaultOptions().
				WithDir(t.TempDir()).
				WithDialOptions([]grpc.DialOption{grpc.WithContextDialer(bs.Dialer), grpc.WithTransportCredentials(insecure.NewCredentials())}),
			)

			err := client.OpenSession(context.Background(), []byte(`immudb`), []byte(`immudb`), "defaultdb")
			require.NoError(t, err)

			tx, err := client.NewTx(context.Background())
			require.NoError(t, err)

			time.Sleep(time.Millisecond * time.Duration(rand.Intn(1000)))

			th, err := tx.Commit(context.Background())
			require.NoError(t, err)
			require.Nil(t, th.Header)
			require.Equal(t, uint32(0), th.UpdatedRows)

			err = client.CloseSession(context.Background())
			require.NoError(t, err)
		}()
	}
	wg.Wait()
}

func TestSession_CreateDBFromSQLStmts(t *testing.T) {
	_, client, ctx := setupTestServerAndClient(t)

	_, err := client.SQLExec(ctx, `
		CREATE DATABASE db1;
		USE db1;
		
		BEGIN TRANSACTION;
			CREATE TABLE table1(id INTEGER AUTO_INCREMENT, title VARCHAR, PRIMARY KEY id);

			INSERT INTO table1(title) VALUES ('title1'), ('title2');
		COMMIT;
	`, nil)
	require.NoError(t, err)

	err = client.CloseSession(context.Background())
	require.NoError(t, err)
}

/*
Copyright 2025 Codenotary Inc. All rights reserved.

SPDX-License-Identifier: BUSL-1.1
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    https://mariadb.com/bsl11/

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package immuadmin

import (
	"bytes"
	"context"
	"io/ioutil"
	"log"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/codenotary/immudb/cmd/cmdtest"
	"github.com/codenotary/immudb/cmd/immuadmin/command/stats/statstest"
	"github.com/codenotary/immudb/pkg/client"
	"github.com/codenotary/immudb/pkg/client/clienttest"
	"github.com/codenotary/immudb/pkg/client/tokenservice"
	"github.com/codenotary/immudb/pkg/server"
	"github.com/codenotary/immudb/pkg/server/servertest"
)

func TestStats_Status(t *testing.T) {

	options := server.DefaultOptions().WithAuth(true).WithDir(t.TempDir())
	bs := servertest.NewBufconnServer(options)

	bs.Start()
	defer bs.Stop()

	dialOptions := []grpc.DialOption{
		grpc.WithContextDialer(bs.Dialer), grpc.WithTransportCredentials(insecure.NewCredentials()),
	}
	cliopt := Options().WithDir(t.TempDir())
	cliopt.DialOptions = dialOptions
	clientb, _ := client.NewImmuClient(cliopt)
	tkf := cmdtest.RandString()
	cl := commandline{
		options:        cliopt,
		immuClient:     clientb,
		passwordReader: &clienttest.PasswordReaderMock{},
		context:        context.Background(),
		ts:             tokenservice.NewFileTokenService().WithHds(newHomedirServiceMock()).WithTokenFileName(tkf),
	}
	cmd, _ := cl.NewCmd()

	cl.status(cmd)

	b := bytes.NewBufferString("")
	cmd.SetOut(b)
	cmd.SetArgs([]string{"status"})

	// remove ConfigChain method to avoid override options
	cmd.PersistentPreRunE = nil
	statcmd := cmd.Commands()[0]
	statcmd.PersistentPreRunE = nil

	cmd.Execute()
	out, err := ioutil.ReadAll(b)
	require.NoError(t, err)
	assert.Contains(t, string(out), "OK - server is reachable and responding to queries")
	assert.Contains(t, string(out), "Version")
	assert.Contains(t, string(out), "Up time")
	assert.Contains(t, string(out), "Databases")
	assert.Contains(t, string(out), "Transactions")
}

func TestStats_StatsText(t *testing.T) {
	options := server.DefaultOptions().WithAuth(true).WithDir(t.TempDir())
	bs := servertest.NewBufconnServer(options)

	bs.Start()
	defer bs.Stop()

	handler := http.NewServeMux()
	handler.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
		if _, err := w.Write(statstest.StatsResponse); err != nil {
			log.Fatal(err)
		}
	})
	server := &http.Server{Addr: ":9497", Handler: handler}
	go server.ListenAndServe()
	defer server.Close()

	dialOptions := []grpc.DialOption{
		grpc.WithContextDialer(bs.Dialer), grpc.WithTransportCredentials(insecure.NewCredentials()),
	}
	cliopt := Options().WithDir(t.TempDir())
	cliopt.DialOptions = dialOptions
	cliopt.Address = "127.0.0.1"
	clientb, _ := client.NewImmuClient(cliopt)
	tkf := cmdtest.RandString()
	cl := commandline{
		options:        cliopt,
		immuClient:     clientb,
		passwordReader: &clienttest.PasswordReaderMock{},
		context:        context.Background(),
		ts:             tokenservice.NewFileTokenService().WithHds(newHomedirServiceMock()).WithTokenFileName(tkf),
	}
	cmd, _ := cl.NewCmd()

	cl.stats(cmd)

	b := bytes.NewBufferString("")
	cmd.SetOut(b)
	cmd.SetArgs([]string{"stats", "--text"})

	// remove ConfigChain method to avoid override options
	cmd.PersistentPreRunE = nil
	statcmd := cmd.Commands()[0]
	statcmd.PersistentPreRunE = nil

	cmd.Execute()
	out, err := ioutil.ReadAll(b)
	require.NoError(t, err)
	assert.Contains(t, string(out), "Database")
}

func TestStats_StatsRaw(t *testing.T) {
	options := server.DefaultOptions().WithAuth(true).WithDir(t.TempDir())
	bs := servertest.NewBufconnServer(options)

	bs.Start()
	defer bs.Stop()

	handler := http.NewServeMux()
	handler.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write(statstest.StatsResponse)
	})
	server := &http.Server{Addr: ":9497", Handler: handler}
	go server.ListenAndServe()

	defer server.Close()

	dialOptions := []grpc.DialOption{
		grpc.WithContextDialer(bs.Dialer), grpc.WithTransportCredentials(insecure.NewCredentials()),
	}
	cliopt := Options().WithDir(t.TempDir())
	cliopt.DialOptions = dialOptions
	cliopt.Address = "127.0.0.1"
	clientb, _ := client.NewImmuClient(cliopt)
	tkf := cmdtest.RandString()
	cl := commandline{
		options:        cliopt,
		immuClient:     clientb,
		passwordReader: &clienttest.PasswordReaderMock{},
		context:        context.Background(),
		ts:             tokenservice.NewFileTokenService().WithHds(newHomedirServiceMock()).WithTokenFileName(tkf),
	}
	cmd, _ := cl.NewCmd()
	cl.stats(cmd)

	b := bytes.NewBufferString("")
	cmd.SetOut(b)
	cmd.SetArgs([]string{"stats", "--raw"})

	// remove ConfigChain method to avoid override options
	cmd.PersistentPreRunE = nil
	statcmd := cmd.Commands()[0]
	statcmd.PersistentPreRunE = nil

	cmd.Execute()
	out, err := ioutil.ReadAll(b)
	require.NoError(t, err)
	assert.Contains(t, string(out), "go_gc_duration_seconds")
}

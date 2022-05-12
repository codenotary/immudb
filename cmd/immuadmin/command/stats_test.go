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

package immuadmin

/*
import (
	"bytes"
	"context"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"testing"

	"github.com/codenotary/immudb/cmd/immuadmin/command/stats/statstest"
	"github.com/codenotary/immudb/pkg/client"
	"github.com/codenotary/immudb/pkg/client/clienttest"
	"github.com/codenotary/immudb/pkg/server"
	"github.com/codenotary/immudb/pkg/server/servertest"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"
)

func TestStats_Status(t *testing.T) {
	options := server.DefaultOptions().WithAuth(true)
	bs := servertest.NewBufconnServer(options)

	bs.Start()
	defer bs.Stop()

	defer os.RemoveAll(options.Dir)
	defer os.Remove(".state-")

	dialOptions := []grpc.DialOption{
		grpc.WithContextDialer(bs.Dialer), grpc.WithInsecure(),
	}
	cliopt := Options()
	cliopt.DialOptions = dialOptions
	clientb, _ := client.NewImmuClient(cliopt)
	cl := commandline{
		options:        cliopt,
		immuClient:     clientb,
		passwordReader: &clienttest.PasswordReaderMock{},
		context:        context.Background(),
		ts:             tokenservice.NewTokenService().WithHds(&clienttest.HomedirServiceMock{}).WithTokenFileName("tokenFileName"),
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
	if err != nil {
		t.Fatal(err)
	}
	assert.Contains(t, string(out), "OK - server is reachable and responding to queries")
}

func TestStats_StatsText(t *testing.T) {
	options := server.DefaultOptions().WithAuth(true)
	bs := servertest.NewBufconnServer(options)

	bs.Start()
	defer bs.Stop()

	defer os.RemoveAll(options.Dir)
	defer os.Remove(".state-")

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
		grpc.WithContextDialer(bs.Dialer), grpc.WithInsecure(),
	}
	cliopt := Options()
	cliopt.DialOptions = dialOptions
	cliopt.Address = "127.0.0.1"
	clientb, _ := client.NewImmuClient(cliopt)
	cl := commandline{
		options:        cliopt,
		immuClient:     clientb,
		passwordReader: &clienttest.PasswordReaderMock{},
		context:        context.Background(),
		ts:             tokenservice.NewTokenService().WithHds(&clienttest.HomedirServiceMock{}).WithTokenFileName("tokenFileName"),
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
	if err != nil {
		t.Fatal(err)
	}
	assert.Contains(t, string(out), "Database path")
}

func TestStats_StatsRaw(t *testing.T) {
	options := server.DefaultOptions().WithAuth(true)
	bs := servertest.NewBufconnServer(options)

	bs.Start()
	defer bs.Stop()

	defer os.RemoveAll(options.Dir)
	defer os.Remove(".state-")

	handler := http.NewServeMux()
	handler.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write(statstest.StatsResponse)
	})
	server := &http.Server{Addr: ":9497", Handler: handler}
	go server.ListenAndServe()

	defer server.Close()

	dialOptions := []grpc.DialOption{
		grpc.WithContextDialer(bs.Dialer), grpc.WithInsecure(),
	}
	cliopt := Options()
	cliopt.DialOptions = dialOptions
	cliopt.Address = "127.0.0.1"
	clientb, _ := client.NewImmuClient(cliopt)
	cl := commandline{
		options:        cliopt,
		immuClient:     clientb,
		passwordReader: &clienttest.PasswordReaderMock{},
		context:        context.Background(),
		ts:             tokenservice.NewTokenService().WithHds(&clienttest.HomedirServiceMock{}).WithTokenFileName("tokenFileName"),
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
	if err != nil {
		t.Fatal(err)
	}
	assert.Contains(t, string(out), "go_gc_duration_seconds")
}
*/

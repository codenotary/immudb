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

package immuadmin

import (
	"bytes"
	"context"
	"github.com/codenotary/immudb/pkg/client"
	"github.com/codenotary/immudb/pkg/client/clienttest"
	"github.com/codenotary/immudb/pkg/server"
	"github.com/codenotary/immudb/pkg/server/servertest"
	"github.com/spf13/cobra"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"
	"io/ioutil"

	"testing"
)

func TestStats_Status(t *testing.T) {
	bs := servertest.NewBufconnServer(server.Options{}.WithAuth(false).WithInMemoryStore(true))
	bs.Start()

	cmd := cobra.Command{}
	dialOptions := []grpc.DialOption{
		grpc.WithContextDialer(bs.Dialer), grpc.WithInsecure(),
	}
	cliopt := Options()
	cliopt.DialOptions = &dialOptions
	clientb, _ := client.NewImmuClient(cliopt)
	cl := commandline{
		options:        cliopt,
		immuClient:     clientb,
		passwordReader: &clienttest.PasswordReaderMock{},
		context:        context.Background(),
		hds:            &clienttest.HomedirServiceMock{},
	}

	cl.status(&cmd)

	b := bytes.NewBufferString("")
	cmd.SetOut(b)
	cmd.SetArgs([]string{"status"})
	cmd.Execute()
	out, err := ioutil.ReadAll(b)
	if err != nil {
		t.Fatal(err)
	}
	assert.Contains(t, string(out), "OK - server is reachable and responding to queries")
}

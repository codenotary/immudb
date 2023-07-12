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

package immuadmin

import (
	"testing"

	"github.com/codenotary/immudb/pkg/auth"
	"github.com/codenotary/immudb/pkg/client"
	"github.com/codenotary/immudb/pkg/server"
	"github.com/codenotary/immudb/pkg/server/servertest"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func TestOptions(t *testing.T) {
	opts := Options()
	assert.IsType(t, &client.Options{}, opts)
}

func TestOptionsMtls(t *testing.T) {
	defer viper.Reset()
	viper.Set("mtls", true)
	opts := Options()
	assert.IsType(t, &client.Options{}, opts)
}

type passwordReaderMock struct {
	Counter int
}

func (pwr *passwordReaderMock) Read(msg string) ([]byte, error) {
	var pw []byte
	if pwr.Counter == 0 {
		pw = []byte(auth.SysAdminPassword)
	} else {
		pw = []byte(`Passw0rd!-`)
	}
	pwr.Counter++
	return pw, nil
}

// Initialize an immudb instance and prepare a command line to connect to it.
func newTestCommandLine(t *testing.T) (*commandline, *cobra.Command) {
	tempDir := t.TempDir()
	options := server.DefaultOptions().WithAuth(true).WithDir(tempDir)
	bs := servertest.NewBufconnServer(options)

	err := bs.Start()
	if err != nil {
		t.Fatalf("starting Bufconn server for immudb failed: %v", err)
	}
	t.Cleanup(func() { bs.Stop() })

	// Create a command line with the dial options to connect to the test server.
	cmdl := NewCommandLine()
	cmdl.dialOptions = []grpc.DialOption{
		grpc.WithContextDialer(bs.Dialer), grpc.WithTransportCredentials(insecure.NewCredentials()),
	}
	pwr := &passwordReaderMock{}
	cmdl.passwordReader = pwr

	// Create command and execute it to initialize command line flags.
	cmd, err := cmdl.NewCmd()
	if err != nil {
		t.Fatalf("initializing cobra command failed: %v", err)
	}
	cmdl.Register(cmd)

	return cmdl, cmd
}

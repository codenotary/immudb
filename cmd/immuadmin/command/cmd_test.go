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
	"io"
	"testing"

	"github.com/codenotary/immudb/pkg/auth"
	"github.com/codenotary/immudb/pkg/client/clienttest"
	"github.com/codenotary/immudb/pkg/fs"
	"github.com/codenotary/immudb/pkg/server"
	"github.com/codenotary/immudb/pkg/server/servertest"
	"github.com/spf13/cobra"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func TestNewCmd(t *testing.T) {
	cmd, _, _ := newCommand()
	assert.IsType(t, cobra.Command{}, *cmd)
}

type passwordReaderMock struct {
	Counter   int
	Loop      bool
	Passwords []string
}

func (pwr *passwordReaderMock) Read(msg string) (pw []byte, err error) {
	if pwr.Loop {
		index := pwr.Counter % len(pwr.Passwords)
		pw = []byte(pwr.Passwords[index])
	} else if pwr.Counter < len(pwr.Passwords) {
		pw = []byte(pwr.Passwords[pwr.Counter])
	} else {
		err = io.EOF
	}
	pwr.Counter++
	return
}
func (pwr *passwordReaderMock) Reset() {
	pwr.Counter = 0
}

func (pwr *passwordReaderMock) SetLoop(loop bool) {
	pwr.Loop = loop
}

// Initialize an immudb instance and initialize the commands.
func newTestCommandLines(t *testing.T) (*commandline, *commandlineBck, *cobra.Command) {
	tempDir := t.TempDir()
	options := server.DefaultOptions().WithAuth(true).WithDir(tempDir)
	bs := servertest.NewBufconnServer(options)

	err := bs.Start()
	if err != nil {
		t.Fatalf("starting Bufconn server for immudb failed: %v", err)
	}
	t.Cleanup(func() { bs.Stop() })

	// Create a command line with the dial options to connect to the test server.
	cmd, cmdl, cmdlBck := newCommand()
	cmdl.dialOptions = []grpc.DialOption{
		grpc.WithContextDialer(bs.Dialer), grpc.WithTransportCredentials(insecure.NewCredentials()),
	}
	// Initialize a mockup password reader with the default password.
	pwr := &passwordReaderMock{
		Counter:   0,
		Passwords: []string{auth.SysAdminPassword},
	}
	cmdl.passwordReader = pwr

	return cmdl, cmdlBck, cmd
}

// Initialize an immudb instance and prepare a command line to connect to it.
func newTestCommandLine(t *testing.T) (*commandline, *cobra.Command) {
	cmdl, _, cmd := newTestCommandLines(t)
	return cmdl, cmd
}

// Initialize an immudb instance and prepare a backup command line to connect to it.
func newTestCommandLineBck(t *testing.T) (*commandlineBck, *cobra.Command) {
	_, cmdlBck, cmd := newTestCommandLines(t)

	// Create a mockup immudb daemon.
	daemMock := defaultDaemonMock()
	cmdlBck.Backupper = &backupper{
		daemon: daemMock,
		os:     cmdlBck.os,
		copier: fs.NewStandardCopier(),
		tarer:  fs.NewStandardTarer(),
		ziper:  fs.NewStandardZiper(),
	}

	// Create a mockup terminal reader that will only answer yes.
	termReaderMock := &clienttest.TerminalReaderMock{
		ReadFromTerminalYNF: func(def string) (selected string, err error) {
			return "Y", nil
		},
	}
	cmdlBck.TerminalReader = termReaderMock

	return cmdlBck, cmd
}

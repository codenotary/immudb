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

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	"github.com/stretchr/testify/assert"

	"github.com/codenotary/immudb/cmd/helper"
	"github.com/codenotary/immudb/pkg/client"
	"github.com/codenotary/immudb/pkg/server"
	"github.com/codenotary/immudb/pkg/server/servertest"
)

var options = server.DefaultOptions().WithAuth(true)
var bs = servertest.NewBufconnServer(options)

var ErrExpectedFailure = errors.New("expected failure")

func TestMain(m *testing.M) {
	os.RemoveAll("data")
	bs.Start()
	defer bs.Stop()

	defer os.RemoveAll(options.Dir)
	defer os.Remove(".state-")

	os.Exit(m.Run())
}

func getCmdline() *commandline {
	dialOptions := []grpc.DialOption{
		grpc.WithContextDialer(bs.Dialer), grpc.WithInsecure(),
	}
	cliopt := Options().WithDialOptions(dialOptions)

	clientb, _ := client.NewImmuClient(cliopt)
	token, err := clientb.Login(context.Background(), []byte("immudb"), []byte("immudb"))
	if err != nil {
		return nil
	}
	md := metadata.Pairs("authorization", token.Token)
	ctx := metadata.NewOutgoingContext(context.Background(), md)
	return &commandline{
		config:     helper.Config{Name: "immuadmin"},
		options:    cliopt,
		immuClient: clientb,
		context:    ctx,
	}
}

func TestRestore(t *testing.T) {
	fmt.Println("Restore")
	cl := commandlineHotBck{}
	cmd, _ := cl.NewCmd()

	cmdl := commandlineHotBck{commandline: *getCmdline()}
	cmdl.hotBackup(cmd)
	cmdl.hotRestore(cmd)

	output := bytes.NewBufferString("")
	cmd.SetOut(output)
	cmd.SetErr(output)

	// disable connects/disconnects, cmd already contains connected immudb client
	cmds := cmd.Commands()
	cmds[0].PersistentPreRunE = nil
	cmds[0].PersistentPostRun = nil
	cmds[1].PersistentPreRunE = nil
	cmds[1].PersistentPostRun = nil

	// full restore (1-10)
	cmd.SetArgs([]string{"hot-restore", "test", "-i", "testdata/1-10.backup"})
	err := cmd.Execute()
	if err != nil {
		t.Fatal(err)
	}
	out, err := ioutil.ReadAll(output)
	if err != nil {
		t.Fatal(err)
	}
	assert.Contains(t, string(out), "Restored transactions from 1 to 10")

	// append w/o append flag (10-11), should fail
	cmd.SetArgs([]string{"hot-restore", "test", "-i", "testdata/10-11.backup"})
	err = cmd.Execute()
	if err == nil {
		t.Fatal(ErrExpectedFailure)
	}
	out, err = ioutil.ReadAll(output)
	if err != nil {
		t.Fatal(err)
	}
	assert.Contains(t, string(out), "Error: cannot restore to non-empty database without --append flag")

	// gap in transactions (last in DB - 10, first in file - 12), should fail
	cmd.SetArgs([]string{"hot-restore", "test", "-i", "testdata/12-14.backup", "--append"})
	err = cmd.Execute()
	if err == nil {
		t.Fatal(ErrExpectedFailure)
	}
	out, err = ioutil.ReadAll(output)
	if err != nil {
		t.Fatal(err)
	}
	assert.Contains(t, string(out), "Error: there is a gap of 1 transaction(s) between database and file - restore not possible")

	// append with overlap (10-11), txn 10 is verified. txn 11 is restored
	cmd.SetArgs([]string{"hot-restore", "test", "-i", "testdata/10-11.backup", "--append", "--force-replica"})
	err = cmd.Execute()
	if err != nil {
		t.Fatal(err)
	}
	out, err = ioutil.ReadAll(output)
	if err != nil {
		t.Fatal(err)
	}
	assert.Contains(t, string(out), "Restored transaction 11")

	// append without overlap (last in DB - 11, first in file - 12) - 11th txn cannot be verified, should fail
	cmd.SetArgs([]string{"hot-restore", "test", "-i", "testdata/12-14.backup", "--append", "--force-replica"})
	err = cmd.Execute()
	if err == nil {
		t.Fatal(ErrExpectedFailure)
	}
	out, err = ioutil.ReadAll(output)
	if err != nil {
		t.Fatal(err)
	}
	assert.Contains(t, string(out), "Error: not possible to validate last transaction in DB - use --force to override")

	// append without overlap (last in DB - 11, first in file - 12) - 11th txn cannot be verified, forced
	cmd.SetArgs([]string{"hot-restore", "test", "-i", "testdata/12-14.backup", "--append", "--force", "--force-replica"})
	err = cmd.Execute()
	if err != nil {
		t.Fatal(err)
	}
	out, err = ioutil.ReadAll(output)
	if err != nil {
		t.Fatal(err)
	}
	assert.Contains(t, string(out), "Restored transactions from 12 to 14")

	// duplicate restore, all txns already in DB, nothing restored
	cmd.SetArgs([]string{"hot-restore", "test", "-i", "testdata/12-14.backup", "--append", "--force-replica"})
	err = cmd.Execute()
	if err != nil {
		t.Fatal(err)
	}
	out, err = ioutil.ReadAll(output)
	if err != nil {
		t.Fatal(err)
	}
	assert.Contains(t, string(out), "Target database is up-to-date, nothing restored")

	// txn 14 in file doesn't match the txn 14 in database, should fail
	cmd.SetArgs([]string{"hot-restore", "test", "-i", "testdata/14-15_modified.backup", "--append", "--force-replica"})
	err = cmd.Execute()
	if err == nil {
		t.Fatal(ErrExpectedFailure)
	}
	out, err = ioutil.ReadAll(output)
	if err != nil {
		t.Fatal(err)
	}
	assert.Contains(t, string(out), "Error: checksums for tx 14 in backup file and database differ - cannot append data to the database")

	// verify backup file
	cmd.SetArgs([]string{"hot-restore", "--verify-only", "-i", "testdata/1-10.backup"})
	err = cmd.Execute()
	if err != nil {
		t.Fatal(err)
	}
	out, err = ioutil.ReadAll(output)
	if err != nil {
		t.Fatal(err)
	}
	assert.Contains(t, string(out), "Backup file contains transactions from 1 to 10")
}

func TestBackup(t *testing.T) {
	fmt.Println("Backup")
	cl := commandlineHotBck{}
	cmd, _ := cl.NewCmd()

	cmdl := commandlineHotBck{commandline: *getCmdline()}
	cmdl.hotBackup(cmd)
	cmdl.hotRestore(cmd)

	output := bytes.NewBufferString("")
	cmd.SetOut(output)
	cmd.SetErr(output)

	// disable connects/disconnects, cmd already contains connected immudb client
	cmds := cmd.Commands()
	cmds[0].PersistentPreRunE = nil
	cmds[0].PersistentPostRun = nil
	cmds[1].PersistentPreRunE = nil
	cmds[1].PersistentPostRun = nil

	// restore (1-10)
	cmd.SetArgs([]string{"hot-restore", "test1", "-i", "testdata/1-10.backup"})
	err := cmd.Execute()
	if err != nil {
		t.Fatal(err)
	}

	// full backup (1-10)
	os.Remove("full.backup")
	cmd.SetArgs([]string{"hot-backup", "test1", "-o", "full.backup"})
	err = cmd.Execute()
	if err != nil {
		t.Fatal(err)
	}
	out, err := ioutil.ReadAll(output)
	if err != nil {
		t.Fatal(err)
	}
	assert.Contains(t, string(out), "Backing up transactions from 1 to 10")

	// partial backup (5-10)
	os.Remove("1-5.backup")
	cmd.SetArgs([]string{"hot-backup", "test1", "--start-tx", "5", "-o", "1-5.backup"})
	err = cmd.Execute()
	if err != nil {
		t.Fatal(err)
	}
	out, err = ioutil.ReadAll(output)
	if err != nil {
		t.Fatal(err)
	}
	assert.Contains(t, string(out), "Backing up transactions from 5 to 10")

	// restore (11)
	cmd.SetArgs([]string{"hot-restore", "test1", "--append", "-i", "testdata/10-11.backup", "--force-replica"})
	err = cmd.Execute()
	if err != nil {
		t.Fatal(err)
	}

	// append txn 11 to file - require --append flag, should fail
	cmd.SetArgs([]string{"hot-backup", "test1", "--start-tx", "1", "-o", "full.backup"})
	err = cmd.Execute()
	if err == nil {
		t.Fatal(ErrExpectedFailure)
	}
	out, err = ioutil.ReadAll(output)
	if err != nil {
		t.Fatal(err)
	}
	assert.Contains(t, string(out), "Error: file already exists, use --append option to append new data to file")

	// append txn 11 to file with --append flag
	cmd.SetArgs([]string{"hot-backup", "test1", "--append", "-o", "full.backup"})
	err = cmd.Execute()
	if err != nil {
		t.Fatal(err)
	}
	out, err = ioutil.ReadAll(output)
	if err != nil {
		t.Fatal(err)
	}
	assert.Contains(t, string(out), "Backing up transaction 11")

	// restore (12-14)
	cmd.SetArgs([]string{"hot-restore", "test1", "--append", "--force", "-i", "testdata/12-14.backup"})
	err = cmd.Execute()
	if err != nil {
		t.Fatal(err)
	}

	// append txn 12-14 to file
	cmd.SetArgs([]string{"hot-backup", "test1", "--append", "-o", "full.backup"})
	err = cmd.Execute()
	if err != nil {
		t.Fatal(err)
	}
	out, err = ioutil.ReadAll(output)
	if err != nil {
		t.Fatal(err)
	}
	assert.Contains(t, string(out), "Backing up transactions from 12 to 14")

	// full restore (1-13) to second DB
	cmd.SetArgs([]string{"hot-restore", "test2", "--append", "-i", "testdata/1-13.backup"})
	err = cmd.Execute()
	if err != nil {
		t.Fatal(err)
	}

	// add modified txn 14 to second DB
	cmd.SetArgs([]string{"hot-restore", "test2", "--append", "-i", "testdata/14-15_modified.backup"})
	err = cmd.Execute()
	if err != nil {
		t.Fatal(err)
	}

	// append txn 15 to file from second DB, should fail because txn 14 in DB and file differ
	cmd.SetArgs([]string{"hot-backup", "test2", "--append", "-o", "full.backup"})
	err = cmd.Execute()
	if err == nil {
		t.Fatal(ErrExpectedFailure)
	}
	out, err = ioutil.ReadAll(output)
	if err != nil {
		t.Fatal(err)
	}
	assert.Contains(t, string(out), "Error: checksums for transaction 14 in backup file and database differ - probably file was created from different database")
}

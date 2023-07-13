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
	"bytes"
	"fmt"
	"io/ioutil"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHotRestore(t *testing.T) {
	fmt.Println("Restore")
	cmdl, cmd := newTestCommandLine(t)
	// Get the password reader from the command line.
	pwr, ok := cmdl.passwordReader.(*passwordReaderMock)
	if !assert.True(t, ok, "The password reader of the commandline should be a mock passwordReader during testing.") {
		t.FailNow()
	}

	output := bytes.NewBufferString("")
	cmd.SetOut(output)
	cmd.SetErr(output)

	// full restore (1-10)
	cmd.SetArgs([]string{"hot-restore", "test", "-i", "testdata/1-10.backup"})
	err := cmd.Execute()
	require.NoError(t, err)

	out, err := ioutil.ReadAll(output)
	require.NoError(t, err)
	assert.Contains(t, string(out), "Restored transactions from 1 to 10")
	pwr.Reset()

	// append w/o append flag (10-11), should fail
	cmd.SetArgs([]string{"hot-restore", "test", "-i", "testdata/10-11.backup"})
	err = cmd.Execute()
	require.Error(t, err)

	out, err = ioutil.ReadAll(output)
	require.NoError(t, err)
	assert.Contains(t, string(out), "Error: cannot restore to non-empty database without --append flag")
	pwr.Reset()

	// gap in transactions (last in DB - 10, first in file - 12), should fail
	cmd.SetArgs([]string{"hot-restore", "test", "-i", "testdata/12-14.backup", "--append"})
	err = cmd.Execute()
	require.Error(t, err)

	out, err = ioutil.ReadAll(output)
	require.NoError(t, err)
	assert.Contains(t, string(out), "Error: there is a gap of 1 transaction(s) between database and file - restore not possible")
	pwr.Reset()

	// append with overlap (10-11), txn 10 is verified. txn 11 is restored
	cmd.SetArgs([]string{"hot-restore", "test", "-i", "testdata/10-11.backup", "--append", "--force-replica"})
	err = cmd.Execute()
	require.NoError(t, err)

	out, err = ioutil.ReadAll(output)
	require.NoError(t, err)
	assert.Contains(t, string(out), "Restored transaction 11")
	pwr.Reset()

	// append without overlap (last in DB - 11, first in file - 12) - 11th txn cannot be verified, should fail
	cmd.SetArgs([]string{"hot-restore", "test", "-i", "testdata/12-14.backup", "--append", "--force-replica"})
	err = cmd.Execute()
	require.Error(t, err)
	out, err = ioutil.ReadAll(output)
	require.NoError(t, err)
	assert.Contains(t, string(out), "Error: not possible to validate last transaction in DB - use --force to override")
	pwr.Reset()

	// append without overlap (last in DB - 11, first in file - 12) - 11th txn cannot be verified, forced
	cmd.SetArgs([]string{"hot-restore", "test", "-i", "testdata/12-14.backup", "--append", "--force", "--force-replica"})
	err = cmd.Execute()
	require.NoError(t, err)
	out, err = ioutil.ReadAll(output)
	require.NoError(t, err)
	assert.Contains(t, string(out), "Restored transactions from 12 to 14")
	pwr.Reset()

	// duplicate restore, all txns already in DB, nothing restored
	cmd.SetArgs([]string{"hot-restore", "test", "-i", "testdata/12-14.backup", "--append", "--force-replica"})
	err = cmd.Execute()
	require.NoError(t, err)
	out, err = ioutil.ReadAll(output)
	require.NoError(t, err)
	assert.Contains(t, string(out), "Target database is up-to-date, nothing restored")
	pwr.Reset()

	// txn 14 in file doesn't match the txn 14 in database, should fail
	cmd.SetArgs([]string{"hot-restore", "test", "-i", "testdata/14-15_modified.backup", "--append", "--force-replica"})
	err = cmd.Execute()
	require.Error(t, err)
	out, err = ioutil.ReadAll(output)
	require.NoError(t, err)
	assert.Contains(t, string(out), "Error: checksums for tx 14 in backup file and database differ - cannot append data to the database")
	pwr.Reset()

	// verify backup file
	cmd.SetArgs([]string{"hot-restore", "--verify-only", "-i", "testdata/1-10.backup"})
	err = cmd.Execute()
	require.NoError(t, err)
	out, err = ioutil.ReadAll(output)
	require.NoError(t, err)
	assert.Contains(t, string(out), "Backup file contains transactions from 1 to 10")
	pwr.Reset()
}

func TestHotBackup(t *testing.T) {
	fmt.Println("Backup")
	cmdl, cmd := newTestCommandLine(t)
	// Get the password reader from the command line.
	pwr, ok := cmdl.passwordReader.(*passwordReaderMock)
	if !assert.True(t, ok, "The password reader of the commandline should be a mock passwordReader during testing.") {
		t.FailNow()
	}

	output := bytes.NewBufferString("")
	cmd.SetOut(output)
	cmd.SetErr(output)

	tmpDir := t.TempDir()
	backupFile_full := filepath.Join(tmpDir, "full.backup")
	backupFile_1_5 := filepath.Join(tmpDir, "1-5.backup")

	// restore (1-10)
	cmd.SetArgs([]string{"hot-restore", "test1", "-i", "testdata/1-10.backup"})
	err := cmd.Execute()
	require.NoError(t, err)
	pwr.Reset()

	// full backup (1-10)
	cmd.SetArgs([]string{"hot-backup", "test1", "-o", backupFile_full})
	err = cmd.Execute()
	require.NoError(t, err)
	out, err := ioutil.ReadAll(output)
	require.NoError(t, err)
	assert.Contains(t, string(out), "Backing up transactions from 1 to 10")
	pwr.Reset()

	// partial backup (5-10)
	cmd.SetArgs([]string{"hot-backup", "test1", "--start-tx", "5", "-o", backupFile_1_5})
	err = cmd.Execute()
	require.NoError(t, err)
	out, err = ioutil.ReadAll(output)
	require.NoError(t, err)
	assert.Contains(t, string(out), "Backing up transactions from 5 to 10")
	pwr.Reset()

	// restore (11)
	cmd.SetArgs([]string{"hot-restore", "test1", "--append", "-i", "testdata/10-11.backup", "--force-replica"})
	err = cmd.Execute()
	require.NoError(t, err)
	pwr.Reset()

	// append txn 11 to file - require --append flag, should fail

	cmd.SetArgs([]string{"hot-backup", "test1", "--start-tx", "1", "-o", backupFile_full})
	err = cmd.Execute()
	require.Error(t, err)
	out, err = ioutil.ReadAll(output)
	require.NoError(t, err)
	assert.Contains(t, string(out), "Error: file already exists, use --append option to append new data to file")
	pwr.Reset()

	// append txn 11 to file with --append flag
	cmd.SetArgs([]string{"hot-backup", "test1", "--append", "-o", backupFile_full})
	err = cmd.Execute()
	require.NoError(t, err)
	out, err = ioutil.ReadAll(output)
	require.NoError(t, err)
	assert.Contains(t, string(out), "Backing up transaction 11")
	pwr.Reset()

	// restore (12-14)
	cmd.SetArgs([]string{"hot-restore", "test1", "--append", "--force", "-i", "testdata/12-14.backup"})
	err = cmd.Execute()
	require.NoError(t, err)
	pwr.Reset()

	// append txn 12-14 to file
	cmd.SetArgs([]string{"hot-backup", "test1", "--append", "-o", backupFile_full})
	err = cmd.Execute()
	require.NoError(t, err)
	out, err = ioutil.ReadAll(output)
	require.NoError(t, err)
	assert.Contains(t, string(out), "Backing up transactions from 12 to 14")
	pwr.Reset()

	// full restore (1-13) to second DB
	cmd.SetArgs([]string{"hot-restore", "test2", "--append", "-i", "testdata/1-13.backup"})
	err = cmd.Execute()
	require.NoError(t, err)
	pwr.Reset()

	// add modified txn 14 to second DB
	cmd.SetArgs([]string{"hot-restore", "test2", "--append", "-i", "testdata/14-15_modified.backup"})
	err = cmd.Execute()
	require.NoError(t, err)
	pwr.Reset()

	// append txn 15 to file from second DB, should fail because txn 14 in DB and file differ
	cmd.SetArgs([]string{"hot-backup", "test2", "--append", "-o", backupFile_full})
	err = cmd.Execute()
	require.Error(t, err)
	out, err = ioutil.ReadAll(output)
	require.NoError(t, err)
	assert.Contains(t, string(out), "Error: checksums for transaction 14 in backup file and database differ - probably file was created from different database")
	pwr.Reset()
}

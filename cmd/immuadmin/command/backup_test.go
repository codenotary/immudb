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
	"context"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/codenotary/immudb/cmd/cmdtest"
	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/codenotary/immudb/pkg/client"
	"github.com/codenotary/immudb/pkg/client/clienttest"
	"github.com/codenotary/immudb/pkg/fs"
	"github.com/codenotary/immudb/pkg/server"
	"github.com/spf13/cobra"
	"github.com/takama/daemon"

	"github.com/stretchr/testify/require"
)

type daemonMock struct {
	GetTemplateF func() string
	SetTemplateF func(string) error
	InstallF     func(...string) (string, error)
	RemoveF      func() (string, error)
	StartF       func() (string, error)
	StopF        func() (string, error)
	StatusF      func() (string, error)
	RunF         func(daemon.Executable) (string, error)
}

func (dm *daemonMock) GetTemplate() string {
	if dm.GetTemplateF == nil {
		return ""
	}
	return dm.GetTemplateF()
}
func (dm *daemonMock) SetTemplate(t string) error {
	if dm.SetTemplateF == nil {
		return nil
	}
	return dm.SetTemplateF(t)
}
func (dm *daemonMock) Install(args ...string) (string, error) {
	if dm.InstallF == nil {
		return "", nil
	}
	return dm.InstallF(args...)
}
func (dm *daemonMock) Remove() (string, error) {
	if dm.RemoveF == nil {
		return "", nil
	}
	return dm.RemoveF()
}
func (dm *daemonMock) Start() (string, error) {
	if dm.StartF == nil {
		return "", nil
	}
	return dm.StartF()
}
func (dm *daemonMock) Stop() (string, error) {
	if dm.StopF == nil {
		return "", nil
	}
	return dm.StopF()
}
func (dm *daemonMock) Status() (string, error) {
	if dm.StatusF == nil {
		return "", nil
	}
	return dm.StatusF()
}
func (dm *daemonMock) Run(e daemon.Executable) (string, error) {
	if dm.RunF == nil {
		return "", nil
	}
	return dm.RunF(e)
}

func defaultDaemonMock() *daemonMock {
	return &daemonMock{
		GetTemplateF: func() string {
			return ""
		},
		SetTemplateF: func(t string) error {
			return nil
		},
		InstallF: func(args ...string) (string, error) {
			return "", nil
		},
		RemoveF: func() (string, error) {
			return "", nil
		},
		StartF: func() (string, error) {
			return "", nil
		},
		StopF: func() (string, error) {
			return "", nil
		},
		StatusF: func() (string, error) {
			return "", nil
		},
		RunF: func(e daemon.Executable) (string, error) {
			return "", nil
		},
	}
}

func TestDumpToFile(t *testing.T) {
	clb, err := newCommandlineBck()
	require.NoError(t, err)
	clb.options = client.DefaultOptions()

	immuClientMock := &clienttest.ImmuClientMock{}
	clb.immuClient = immuClientMock
	clb.newImmuClient = func(*client.Options) (client.ImmuClient, error) {
		return immuClientMock, nil
	}
	immuClientMock.DisconnectF = func() error {
		return nil
	}

	clb.passwordReader = &clienttest.PasswordReaderMock{}
	clb.context = context.Background()

	hds := clienttest.DefaultHomedirServiceMock()
	hds.FileExistsInUserHomeDirF = func(string) (bool, error) {
		return true, nil
	}
	clb.hds = hds

	daemMock := defaultDaemonMock()
	clb.Backupper = &backupper{daemMock}

	termReaderMock := &clienttest.TerminalReaderMock{
		ReadFromTerminalYNF: func(def string) (selected string, err error) {
			return "Y", nil
		},
	}
	clb.TerminalReader = termReaderMock

	dumpFile := "backup_test_dump_output.bkp"
	defer os.Remove(dumpFile)
	errDump := errors.New("dump error")
	immuClientMock.DumpF = func(ctx context.Context, f io.WriteSeeker) (int64, error) {
		return 0, errDump
	}

	collector := new(cmdtest.StdOutCollector)
	clb.onError = func(msg interface{}) {
		dumpLog, err := collector.Stop()
		require.Equal(t, errDump, msg.(error))
		require.NoError(t, err)
		require.Equal(t, "Backup failed.\n", dumpLog)
	}
	cmd := &cobra.Command{}
	cmd.SetArgs([]string{"dump", dumpFile})
	clb.dumpToFile(cmd)

	require.NoError(t, collector.Start())
	require.NoError(t, cmd.Execute())
	collector.Stop()

	immuClientMock.DumpF = func(ctx context.Context, f io.WriteSeeker) (int64, error) {
		return 0, nil
	}
	require.NoError(t, collector.Start())
	require.Nil(t, cmd.Execute())
	dumpLog, err := collector.Stop()
	require.Nil(t, err)
	require.Equal(t, "Database is empty.\n", dumpLog)

	immuClientMock.DumpF = func(ctx context.Context, f io.WriteSeeker) (int64, error) {
		return 1, nil
	}
	require.NoError(t, collector.Start())
	require.Nil(t, cmd.Execute())
	dumpLog, err = collector.Stop()
	require.Nil(t, err)
	require.Equal(t, fmt.Sprintf("SUCCESS: 1 key-value entries were backed-up to file %s\n", dumpFile), dumpLog)
}

func deleteBackupFiles(prefix string) {
	files, _ := filepath.Glob(fmt.Sprintf("./%s_bkp_*", prefix))
	for _, f := range files {
		os.RemoveAll(f)
	}
}

func TestBackup(t *testing.T) {
	clb, err := newCommandlineBck()
	require.NoError(t, err)
	clb.options = client.DefaultOptions()

	loginFOK := func(context.Context, []byte, []byte) (*schema.LoginResponse, error) {
		return &schema.LoginResponse{Token: []byte("token")}, nil
	}
	immuClientMock := &clienttest.ImmuClientMock{
		LoginF: loginFOK,
	}
	clb.immuClient = immuClientMock
	clb.newImmuClient = func(*client.Options) (client.ImmuClient, error) {
		return immuClientMock, nil
	}
	immuClientMock.DisconnectF = func() error {
		return nil
	}

	pwReaderMock := &clienttest.PasswordReaderMock{}
	clb.passwordReader = pwReaderMock
	clb.context = context.Background()

	hds := clienttest.DefaultHomedirServiceMock()
	hds.FileExistsInUserHomeDirF = func(string) (bool, error) {
		return true, nil
	}
	clb.hds = hds

	daemMock := defaultDaemonMock()
	clb.Backupper = &backupper{daemMock}

	okReadFromTerminalYNF := func(def string) (selected string, err error) {
		return "Y", nil
	}
	termReaderMock := &clienttest.TerminalReaderMock{
		ReadFromTerminalYNF: okReadFromTerminalYNF,
	}
	clb.TerminalReader = termReaderMock

	dbDir := "backup_test_db_dir"
	require.NoError(t, os.Mkdir(dbDir, 0755))
	defer os.Remove(dbDir)

	collector := new(cmdtest.StdOutCollector)
	clb.onError = func(msg interface{}) {
		require.Empty(t, msg.(error))
	}

	cmd := &cobra.Command{}
	cmd.SetArgs([]string{
		"backup",
		fmt.Sprintf("--dbdir=%s", dbDir),
	})
	defer deleteBackupFiles(dbDir)
	clb.backup(cmd)

	require.NoError(t, collector.Start())
	require.Nil(t, cmd.Execute())
	backupLog, err := collector.Stop()
	require.NoError(t, err)
	require.Contains(t, backupLog, "Database backup created: ")

	cmd = &cobra.Command{}
	cmd.SetArgs([]string{
		"backup",
		fmt.Sprintf("--dbdir=%s", dbDir),
		"--uncompressed",
	})
	clb.backup(cmd)
	require.NoError(t, collector.Start())
	require.Nil(t, cmd.Execute())
	backupLog, err = collector.Stop()
	require.NoError(t, err)
	require.Contains(t, backupLog, "Database backup created: ")

	nokReadFromTerminalYNF := func(def string) (selected string, err error) {
		return "N", nil
	}
	termReaderMock.ReadFromTerminalYNF = nokReadFromTerminalYNF
	clb.onError = func(msg interface{}) {
		require.Equal(t, "Canceled", msg.(error).Error())
	}
	require.NoError(t, cmd.Execute())
	cmd.SetArgs([]string{
		"backup",
		fmt.Sprintf("--dbdir=%s", dbDir),
		"--manual-stop-start",
	})
	clb.onError = func(msg interface{}) {
		require.Equal(t, "Canceled", msg.(error).Error())
	}
	require.NoError(t, cmd.Execute())
	termReaderMock.ReadFromTerminalYNF = okReadFromTerminalYNF

	cmd = &cobra.Command{}
	cmd.SetArgs([]string{
		"backup",
		fmt.Sprintf("--dbdir=%s", dbDir),
	})
	pwReadErr := errors.New("password read error")
	errPwReadF := func(msg string) ([]byte, error) {
		return nil, pwReadErr
	}
	pwReaderMock.ReadF = errPwReadF
	clb.onError = func(msg interface{}) {
		require.Equal(t, pwReadErr, msg)
	}
	clb.backup(cmd)
	require.NoError(t, cmd.Execute())
	pwReaderMock.ReadF = nil

	errLogin := errors.New("login error")
	immuClientMock.LoginF = func(context.Context, []byte, []byte) (*schema.LoginResponse, error) {
		return nil, errLogin
	}
	clb.onError = func(msg interface{}) {
		require.Equal(t, errLogin, msg)
	}
	require.NoError(t, cmd.Execute())
	immuClientMock.LoginF = loginFOK

	cmd.SetArgs([]string{
		"backup",
		fmt.Sprintf("--dbdir=%s", "."),
	})
	expectedErrMsg :=
		"cannot backup the current directory, please specify a subdirectory, for example ./data"
	clb.onError = func(msg interface{}) {
		require.Equal(t, expectedErrMsg, msg.(error).Error())
	}
	require.NoError(t, cmd.Execute())

	notADir := dbDir + "_not_a_dir"
	cmd.SetArgs([]string{
		"backup",
		fmt.Sprintf("--dbdir=%s", notADir),
	})
	expectedErrMsg =
		"stat backup_test_db_dir_not_a_dir: no such file or directory"
	clb.onError = func(msg interface{}) {
		require.Equal(t, expectedErrMsg, msg.(error).Error())
	}
	require.NoError(t, cmd.Execute())
	_, err = os.Create(notADir)
	require.NoError(t, err)
	defer os.Remove(notADir)
	expectedErrMsg =
		"backup_test_db_dir_not_a_dir is not a directory"
	clb.onError = func(msg interface{}) {
		require.Equal(t, expectedErrMsg, msg.(error).Error())
	}
	require.NoError(t, cmd.Execute())

	cmd.SetArgs([]string{
		"backup",
		fmt.Sprintf("--dbdir=%s", dbDir),
	})
	daemMock.StopF = func() (string, error) {
		return "", errors.New("daemon stop error")
	}
	clb.onError = func(msg interface{}) {
		require.Equal(t, "error stopping immudb server: daemon stop error", msg.(error).Error())
	}
	clb.backup(cmd)
	require.NoError(t, cmd.Execute())
	daemMock.StopF = nil

	daemMock.StartF = func() (string, error) {
		return "", errors.New("daemon start error")
	}
	clb.onError = func(msg interface{}) {
	}
	clb.backup(cmd)
	collector.CaptureStderr = true
	require.NoError(t, collector.Start())
	require.NoError(t, cmd.Execute())
	backupLog, err = collector.Stop()
	require.NoError(t, err)
	require.Contains(t, backupLog, "error restarting immudb server: daemon start error")
	daemMock.StartF = nil
	collector.CaptureStderr = false
}

func TestRestore(t *testing.T) {
	clb, err := newCommandlineBck()
	require.NoError(t, err)
	clb.options = client.DefaultOptions()

	loginFOK := func(context.Context, []byte, []byte) (*schema.LoginResponse, error) {
		return &schema.LoginResponse{Token: []byte("token")}, nil
	}
	immuClientMock := &clienttest.ImmuClientMock{
		LoginF: loginFOK,
	}
	clb.immuClient = immuClientMock
	clb.newImmuClient = func(*client.Options) (client.ImmuClient, error) {
		return immuClientMock, nil
	}
	immuClientMock.DisconnectF = func() error {
		return nil
	}

	pwReaderMock := &clienttest.PasswordReaderMock{}
	clb.passwordReader = pwReaderMock
	clb.context = context.Background()

	hds := clienttest.DefaultHomedirServiceMock()
	hds.FileExistsInUserHomeDirF = func(string) (bool, error) {
		return true, nil
	}
	clb.hds = hds

	daemMock := defaultDaemonMock()
	clb.Backupper = &backupper{daemMock}

	okReadFromTerminalYNF := func(def string) (selected string, err error) {
		return "Y", nil
	}
	termReaderMock := &clienttest.TerminalReaderMock{
		ReadFromTerminalYNF: okReadFromTerminalYNF,
	}
	clb.TerminalReader = termReaderMock

	dbDirSrc := "restore_test_db_dir_src_bkp_1"
	dbDirDst := "restore_test_db_dir_dst"
	os.RemoveAll(dbDirSrc)
	os.RemoveAll(dbDirDst)
	deleteBackupFiles(dbDirDst)
	defer os.RemoveAll(dbDirSrc)
	defer os.RemoveAll(dbDirDst)
	defer deleteBackupFiles(dbDirDst)
	require.NoError(t, os.Mkdir(dbDirSrc, 0755))
	ioutil.WriteFile(filepath.Join(dbDirSrc, server.IDENTIFIER_FNAME), []byte("src"), 0644)
	require.NoError(t, os.Mkdir(dbDirDst, 0755))
	ioutil.WriteFile(filepath.Join(dbDirDst, server.IDENTIFIER_FNAME), []byte("dst"), 0644)

	// collector := new(cmdtest.StdOutCollector)
	clb.onError = func(msg interface{}) {
		require.Empty(t, msg.(error))
	}

	backupFile := dbDirSrc + ".tar.gz"
	os.Remove(backupFile)
	require.NoError(t, fs.TarIt(dbDirSrc, backupFile))
	defer os.Remove(backupFile)

	backupFile2 := dbDirSrc + ".zip"
	os.Remove(backupFile2)
	require.NoError(t, fs.ZipIt(dbDirSrc, backupFile2, fs.ZipNoCompression))
	defer os.Remove(backupFile2)

	cmd := &cobra.Command{}
	cmd.SetArgs([]string{
		"restore",
		backupFile,
		fmt.Sprintf("--dbdir=./%s", dbDirDst),
	})
	clb.restore(cmd)
	require.NoError(t, cmd.Execute())
	deleteBackupFiles(dbDirDst)

	cmd.SetArgs([]string{
		"restore",
		backupFile2,
		fmt.Sprintf("--dbdir=./%s", dbDirDst),
	})
	clb.restore(cmd)
	require.NoError(t, cmd.Execute())
}

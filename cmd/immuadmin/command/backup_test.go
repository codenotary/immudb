/*
Copyright 2024 Codenotary Inc. All rights reserved.

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

/*
import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	stdos "os"
	"path/filepath"
	"testing"

	"github.com/codenotary/immudb/cmd/helper"
	"github.com/stretchr/testify/assert"

	"github.com/codenotary/immudb/cmd/cmdtest"
	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/codenotary/immudb/pkg/client"
	"github.com/codenotary/immudb/pkg/client/clienttest"
	"github.com/codenotary/immudb/pkg/fs"
	"github.com/codenotary/immudb/pkg/immuos"
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
	os := immuos.NewStandardOS()
	clb, err := newCommandlineBck(os)
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
	clb.ts = tokenservice.NewTokenService().WithHds(hds).WithTokenFileName("testTokenFile")

	daemMock := defaultDaemonMock()
	clb.Backupper = &backupper{
		daemon: daemMock,
		os:     os,
		copier: fs.NewStandardCopier(),
		tarer:  fs.NewStandardTarer(),
		ziper:  fs.NewStandardZiper(),
	}

	termReaderMock := &clienttest.TerminalReaderMock{
		ReadFromTerminalYNF: func(def string) (selected string, err error) {
			return "Y", nil
		},
	}
	clb.TerminalReader = termReaderMock

	dumpFile := "backup_test_dump_output.bkp"
	defer stdos.Remove(dumpFile)
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
	cl := commandline{}
	cmd, _ := cl.NewCmd()
	cmd.SetArgs([]string{"dump", dumpFile})
	clb.dumpToFile(cmd)

	require.NoError(t, collector.Start())

	// remove ConfigChain method to avoid override options
	cmd.PersistentPreRunE = nil
	cmdlist := cmd.Commands()[0]
	cmdlist.PersistentPreRunE = nil

	require.NoError(t, cmd.Execute())
	collector.Stop()

	immuClientMock.DumpF = func(ctx context.Context, f io.WriteSeeker) (int64, error) {
		return 0, nil
	}
	require.NoError(t, collector.Start())
	require.Nil(t, cmd.Execute())
	dumpLog, err := collector.Stop()
	require.NoError(t, err)
	require.Equal(t, "Database is empty.\n", dumpLog)

	immuClientMock.DumpF = func(ctx context.Context, f io.WriteSeeker) (int64, error) {
		return 1, nil
	}
	require.NoError(t, collector.Start())
	require.Nil(t, cmd.Execute())
	dumpLog, err = collector.Stop()
	require.NoError(t, err)
	require.Equal(t, fmt.Sprintf("SUCCESS: 1 key-value entries were backed-up to file %s\n", dumpFile), dumpLog)
}

func deleteBackupFiles(prefix string) {
	files, _ := filepath.Glob(fmt.Sprintf("./%s_bkp_*", prefix))
	for _, f := range files {
		stdos.RemoveAll(f)
	}
}

func TestBackup(t *testing.T) {
	os := immuos.NewStandardOS()
	clb, err := newCommandlineBck(os)
	require.NoError(t, err)
	clb.options = client.DefaultOptions()

	loginFOK := func(context.Context, []byte, []byte) (*schema.LoginResponse, error) {
		return &schema.LoginResponse{Token: "token"}, nil
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
	clb.ts = tokenservice.NewTokenService().WithHds(hds).WithTokenFileName("testTokenFile")

	daemMock := defaultDaemonMock()
	clb.Backupper = &backupper{
		daemon: daemMock,
		os:     os,
		copier: fs.NewStandardCopier(),
		tarer:  fs.NewStandardTarer(),
		ziper:  fs.NewStandardZiper(),
	}

	okReadFromTerminalYNF := func(def string) (selected string, err error) {
		return "Y", nil
	}
	termReaderMock := &clienttest.TerminalReaderMock{
		ReadFromTerminalYNF: okReadFromTerminalYNF,
	}
	clb.TerminalReader = termReaderMock

	dbDir := "backup_test_db_dir"
	require.NoError(t, stdos.Mkdir(dbDir, 0755))
	defer stdos.Remove(dbDir)

	collector := new(cmdtest.StdOutCollector)
	clb.onError = func(msg interface{}) {
		require.Empty(t, msg.(error))
	}

	// success
	cl := commandline{}
	cmd, _ := cl.NewCmd()
	cmd.SetArgs([]string{
		"backup",
		fmt.Sprintf("--dbdir=%s", dbDir),
	})
	defer deleteBackupFiles(dbDir)
	clb.backup(cmd)

	require.NoError(t, collector.Start())

	// remove ConfigChain method to avoid override options
	cmd.PersistentPreRunE = nil
	innercmd := cmd.Commands()[0]
	innercmd.PersistentPreRunE = nil

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

	// remove ConfigChain method to avoid override options
	cmd.PersistentPreRunE = nil
	innercmd = cmd.Commands()[0]
	innercmd.PersistentPreRunE = nil

	require.Nil(t, cmd.Execute())
	backupLog, err = collector.Stop()
	require.NoError(t, err)
	require.Contains(t, backupLog, "Database backup created: ")

	// Abs error (uncompressed backup)
	deleteBackupFiles(dbDir)
	absFOK := os.AbsF
	errAbsUncompressed := "Abs error uncompressed"
	nbAbsCalls := 0
	os.AbsF = func(path string) (string, error) {
		nbAbsCalls++
		if nbAbsCalls > 2 {
			return "", errors.New(errAbsUncompressed)
		}
		return absFOK(path)
	}
	collector.CaptureStderr = true
	require.NoError(t, collector.Start())

	// remove ConfigChain method to avoid override options
	cmd.PersistentPreRunE = nil
	innercmd = cmd.Commands()[0]
	innercmd.PersistentPreRunE = nil

	require.NoError(t, cmd.Execute())
	backupLog, err = collector.Stop()
	require.NoError(t, err)
	require.Contains(t, backupLog, errAbsUncompressed)
	collector.CaptureStderr = false
	os.AbsF = absFOK

	// reset command
	deleteBackupFiles(dbDir)
	cl = commandline{}
	cmd, _ = cl.NewCmd()
	cmd.SetArgs([]string{
		"backup",
		fmt.Sprintf("--dbdir=%s", dbDir),
	})
	clb.backup(cmd)

	// Getwd error
	getwdFOK := os.GetwdF
	errGetwd := errors.New("Getwd error")
	os.GetwdF = func() (string, error) {
		return "", errGetwd
	}
	clb.onError = func(msg interface{}) {
		require.Equal(t, errGetwd, msg)
	}

	// remove ConfigChain method to avoid override options
	cmd.PersistentPreRunE = nil
	innercmd = cmd.Commands()[0]
	innercmd.PersistentPreRunE = nil

	require.NoError(t, cmd.Execute())
	os.GetwdF = getwdFOK

	// Abs error
	errAbs1 := errors.New("Abs error 1")
	os.AbsF = func(path string) (string, error) {
		return "", errAbs1
	}
	clb.onError = func(msg interface{}) {
		require.Equal(t, errAbs1, msg)
	}

	// remove ConfigChain method to avoid override options
	cmd.PersistentPreRunE = nil
	innercmd = cmd.Commands()[0]
	innercmd.PersistentPreRunE = nil

	require.NoError(t, cmd.Execute())

	errAbs2 := errors.New("Abs error 2")
	nbAbsCalls = 0
	os.AbsF = func(path string) (string, error) {
		nbAbsCalls++
		if nbAbsCalls == 1 {
			return absFOK(path)
		}
		return "", errAbs2
	}
	clb.onError = func(msg interface{}) {
		require.Equal(t, errAbs2, msg)
	}

	// remove ConfigChain method to avoid override options
	cmd.PersistentPreRunE = nil
	innercmd = cmd.Commands()[0]
	innercmd.PersistentPreRunE = nil

	require.NoError(t, cmd.Execute())

	os.AbsF = absFOK

	// RemoveAll error
	removeAllFOK := os.RemoveAllF
	errRemoveAll := "RemoveAll error"
	os.RemoveAllF = func(path string) error {
		return errors.New(errRemoveAll)
	}
	clb.onError = func(msg interface{}) {
		require.Empty(t, msg)
	}
	collector.CaptureStderr = true
	require.NoError(t, collector.Start())

	// remove ConfigChain method to avoid override options
	cmd.PersistentPreRunE = nil
	innercmd = cmd.Commands()[0]
	innercmd.PersistentPreRunE = nil

	require.NoError(t, cmd.Execute())
	backupLog, err = collector.Stop()
	require.NoError(t, err)
	require.Contains(t, backupLog, errRemoveAll)
	collector.CaptureStderr = false
	os.RemoveAllF = removeAllFOK

	// Abs error again
	deleteBackupFiles(dbDir)
	errAbs3 := "Abs error 3"
	nbAbsCalls = 0
	os.AbsF = func(path string) (string, error) {
		nbAbsCalls++
		if nbAbsCalls <= 2 {
			return absFOK(path)
		}
		return "", errors.New(errAbs3)
	}
	collector.CaptureStderr = true
	require.NoError(t, collector.Start())

	// remove ConfigChain method to avoid override options
	cmd.PersistentPreRunE = nil
	innercmd = cmd.Commands()[0]
	innercmd.PersistentPreRunE = nil

	require.NoError(t, cmd.Execute())
	backupLog, err = collector.Stop()
	require.NoError(t, err)
	require.Contains(t, backupLog, errAbs3)
	collector.CaptureStderr = false
	os.AbsF = absFOK

	// canceled
	nokReadFromTerminalYNF := func(def string) (selected string, err error) {
		return "N", nil
	}
	termReaderMock.ReadFromTerminalYNF = nokReadFromTerminalYNF
	clb.onError = func(msg interface{}) {
		require.Equal(t, "Canceled", msg.(error).Error())
	}

	// remove ConfigChain method to avoid override options
	cmd.PersistentPreRunE = nil
	innercmd = cmd.Commands()[0]
	innercmd.PersistentPreRunE = nil

	require.NoError(t, cmd.Execute())
	cmd.SetArgs([]string{
		"backup",
		fmt.Sprintf("--dbdir=%s", dbDir),
		"--manual-stop-start",
	})
	clb.onError = func(msg interface{}) {
		require.Equal(t, "Canceled", msg.(error).Error())
	}

	// remove ConfigChain method to avoid override options
	cmd.PersistentPreRunE = nil
	innercmd = cmd.Commands()[0]
	innercmd.PersistentPreRunE = nil

	require.NoError(t, cmd.Execute())
	termReaderMock.ReadFromTerminalYNF = okReadFromTerminalYNF

	// password read error
	cl = commandline{}
	cmd, _ = cl.NewCmd()
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

	// remove ConfigChain method to avoid override options
	cmd.PersistentPreRunE = nil
	innercmd = cmd.Commands()[0]
	innercmd.PersistentPreRunE = nil

	require.NoError(t, cmd.Execute())
	pwReaderMock.ReadF = nil

	// login error
	errLogin := errors.New("login error")
	immuClientMock.LoginF = func(context.Context, []byte, []byte) (*schema.LoginResponse, error) {
		return nil, errLogin
	}
	clb.onError = func(msg interface{}) {
		require.Equal(t, errLogin, msg)
	}

	// remove ConfigChain method to avoid override options
	cmd.PersistentPreRunE = nil
	innercmd = cmd.Commands()[0]
	innercmd.PersistentPreRunE = nil

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

	// remove ConfigChain method to avoid override options
	cmd.PersistentPreRunE = nil
	innercmd = cmd.Commands()[0]
	innercmd.PersistentPreRunE = nil

	require.NoError(t, cmd.Execute())

	// dbdir not exists
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

	// remove ConfigChain method to avoid override options
	cmd.PersistentPreRunE = nil
	innercmd = cmd.Commands()[0]
	innercmd.PersistentPreRunE = nil

	require.NoError(t, cmd.Execute())

	// dbdir not a dir
	_, err = stdos.Create(notADir)
	require.NoError(t, err)
	defer stdos.Remove(notADir)
	expectedErrMsg =
		"backup_test_db_dir_not_a_dir is not a directory"
	clb.onError = func(msg interface{}) {
		require.Equal(t, expectedErrMsg, msg.(error).Error())
	}

	// remove ConfigChain method to avoid override options
	cmd.PersistentPreRunE = nil
	innercmd = cmd.Commands()[0]
	innercmd.PersistentPreRunE = nil

	require.NoError(t, cmd.Execute())

	// daemon stop error
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

	// remove ConfigChain method to avoid override options
	cmd.PersistentPreRunE = nil
	innercmd = cmd.Commands()[0]
	innercmd.PersistentPreRunE = nil

	require.NoError(t, cmd.Execute())
	daemMock.StopF = nil

	// daemon start error
	daemMock.StartF = func() (string, error) {
		return "", errors.New("daemon start error")
	}
	clb.onError = func(msg interface{}) {
	}
	clb.backup(cmd)
	collector.CaptureStderr = true
	require.NoError(t, collector.Start())

	// remove ConfigChain method to avoid override options
	cmd.PersistentPreRunE = nil
	innercmd = cmd.Commands()[0]
	innercmd.PersistentPreRunE = nil

	require.NoError(t, cmd.Execute())
	backupLog, err = collector.Stop()
	require.NoError(t, err)
	require.Contains(t, backupLog, "error restarting immudb server: daemon start error")
	daemMock.StartF = nil
	collector.CaptureStderr = false
}

func TestRestore(t *testing.T) {
	os := immuos.NewStandardOS()
	clb, err := newCommandlineBck(os)
	require.NoError(t, err)
	clb.options = client.DefaultOptions()

	loginFOK := func(context.Context, []byte, []byte) (*schema.LoginResponse, error) {
		return &schema.LoginResponse{Token: "token"}, nil
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
	clb.ts = tokenservice.NewTokenService().WithHds(hds).WithTokenFileName("testTokenFile")

	daemMock := defaultDaemonMock()
	bckpr := &backupper{
		daemon: daemMock,
		os:     os,
		copier: fs.NewStandardCopier(),
		tarer:  fs.NewStandardTarer(),
		ziper:  fs.NewStandardZiper(),
	}
	clb.Backupper = bckpr

	okReadFromTerminalYNF := func(def string) (selected string, err error) {
		return "Y", nil
	}
	termReaderMock := &clienttest.TerminalReaderMock{
		ReadFromTerminalYNF: okReadFromTerminalYNF,
	}
	clb.TerminalReader = termReaderMock

	dbDirSrc := "restore_test_db_dir_src_bkp_1"
	dbDirDst := "restore_test_db_dir_dst"
	stdos.RemoveAll(dbDirSrc)
	stdos.RemoveAll(dbDirDst)
	deleteBackupFiles(dbDirDst)
	defer stdos.RemoveAll(dbDirSrc)
	defer stdos.RemoveAll(dbDirDst)
	defer deleteBackupFiles(dbDirDst)
	require.NoError(t, stdos.Mkdir(dbDirSrc, 0755))
	ioutil.WriteFile(filepath.Join(dbDirSrc, server.IDENTIFIER_FNAME), []byte("src"), 0644)
	require.NoError(t, stdos.Mkdir(dbDirDst, 0755))
	ioutil.WriteFile(filepath.Join(dbDirDst, server.IDENTIFIER_FNAME), []byte("dst"), 0644)

	clb.onError = func(msg interface{}) {
		require.Empty(t, msg.(error))
	}

	backupFile := dbDirSrc + ".tar.gz"
	stdos.Remove(backupFile)
	require.NoError(t, bckpr.tarer.TarIt(dbDirSrc, backupFile))
	defer stdos.Remove(backupFile)

	backupFile2 := dbDirSrc + ".zip"
	stdos.Remove(backupFile2)
	require.NoError(t, bckpr.ziper.ZipIt(dbDirSrc, backupFile2, fs.ZipNoCompression))
	defer stdos.Remove(backupFile2)

	// from .tar.gz
	cl := commandline{}
	cmd, _ := cl.NewCmd()
	cmd.SetArgs([]string{
		"restore",
		backupFile,
		fmt.Sprintf("--dbdir=./%s", dbDirDst),
	})
	clb.restore(cmd)

	// remove ConfigChain method to avoid override options
	cmd.PersistentPreRunE = nil
	innercmd := cmd.Commands()[0]
	innercmd.PersistentPreRunE = nil

	require.NoError(t, cmd.Execute())
	deleteBackupFiles(dbDirDst)

	// from .zip
	cmd.SetArgs([]string{
		"restore",
		backupFile2,
		fmt.Sprintf("--dbdir=./%s", dbDirDst),
	})
	clb.restore(cmd)

	// remove ConfigChain method to avoid override options
	cmd.PersistentPreRunE = nil
	innercmd = cmd.Commands()[0]
	innercmd.PersistentPreRunE = nil

	require.NoError(t, cmd.Execute())

	// stat error
	statFOK := os.StatF
	errStat := errors.New("Stat error")
	os.StatF = func(name string) (stdos.FileInfo, error) {
		return nil, errStat
	}
	clb.onError = func(msg interface{}) {
		require.Equal(t, errStat, msg)
	}

	// remove ConfigChain method to avoid override options
	cmd.PersistentPreRunE = nil
	innercmd = cmd.Commands()[0]
	innercmd.PersistentPreRunE = nil

	require.NoError(t, cmd.Execute())
	os.StatF = statFOK

	// daemon stop error
	daemMock.StopF = func() (string, error) {
		return "", errors.New("daemon stop error")
	}
	clb.onError = func(msg interface{}) {
		require.Equal(t, "error stopping immudb server: daemon stop error", fmt.Sprintf("%v", msg))
	}

	// remove ConfigChain method to avoid override options
	cmd.PersistentPreRunE = nil
	innercmd = cmd.Commands()[0]
	innercmd.PersistentPreRunE = nil

	require.NoError(t, cmd.Execute())
	daemMock.StopF = nil

	// Rename errors
	renameFOK := os.RenameF

	os.RenameF = func(oldpath, newpath string) error {
		return errors.New("Rename error 1")
	}
	clb.onError = func(msg interface{}) {
		require.Contains(t, msg.(error).Error(), "Rename error 1")
	}

	// remove ConfigChain method to avoid override options
	cmd.PersistentPreRunE = nil
	innercmd = cmd.Commands()[0]
	innercmd.PersistentPreRunE = nil

	require.NoError(t, cmd.Execute())

	nbCalls := 0
	os.RenameF = func(oldpath, newpath string) error {
		nbCalls++
		if nbCalls == 1 {
			return nil
		}
		return errors.New("Rename error 2")
	}
	clb.onError = func(msg interface{}) {
		require.Contains(t, msg.(error).Error(), "Rename error 2")
	}

	// remove ConfigChain method to avoid override options
	cmd.PersistentPreRunE = nil
	innercmd = cmd.Commands()[0]
	innercmd.PersistentPreRunE = nil

	require.NoError(t, cmd.Execute())

	os.RenameF = renameFOK
}

func TestCommandlineBck_Register(t *testing.T) {
	c := commandlineBck{}
	cmd := c.Register(&cobra.Command{})
	assert.IsType(t, &cobra.Command{}, cmd)
}

func TestNewCommandLineBck(t *testing.T) {
	cml, err := newCommandlineBck(immuos.NewStandardOS())
	assert.IsType(t, &commandlineBck{}, cml)
	assert.NoError(t, err)
}

func TestCommandlineBck_ConfigChain(t *testing.T) {
	cmd := &cobra.Command{}
	c := commandlineBck{
		commandline: commandline{config: helper.Config{Name: "test"}},
	}
	f := func(cmd *cobra.Command, args []string) error {
		return nil
	}
	cmd.Flags().StringVar(&c.config.CfgFn, "config", "", "config file")
	cc := c.ConfigChain(f)
	err := cc(cmd, []string{})
	assert.NoError(t, err)
}

func TestCommandlineBck_ConfigChainErr(t *testing.T) {
	cmd := &cobra.Command{}

	c := commandlineBck{}
	f := func(cmd *cobra.Command, args []string) error {
		return nil
	}

	cc := c.ConfigChain(f)

	err := cc(cmd, []string{})
	assert.Error(t, err)
}
*/

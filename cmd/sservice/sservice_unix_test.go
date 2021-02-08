/*
Copyright 2021 CodeNotary, Inc. All rights reserved.

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

package sservice

import (
	"io/ioutil"
	"os"
	"os/user"
	"path/filepath"
	"testing"

	"github.com/codenotary/immudb/cmd/immudb/command/immudbcmdtest"
	"github.com/codenotary/immudb/cmd/immudb/command/service/servicetest"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	"github.com/codenotary/immudb/pkg/immuos"
	"github.com/stretchr/testify/assert"
	daem "github.com/takama/daemon"
)

var osMock *immuos.StandardOS

func init() {
	osMock = immuos.NewStandardOS()
	osMock.ChownF = func(name string, uid, gid int) error {
		return nil
	}
	osMock.MkdirAllF = func(path string, perm os.FileMode) error {
		return nil
	}
	osMock.RemoveF = func(name string) error {
		return nil
	}
	osMock.RemoveAllF = func(path string) error {
		return nil
	}
	osMock.IsNotExistF = func(err error) bool {
		return false
	}
	osMock.OpenF = func(name string) (*os.File, error) {
		return ioutil.TempFile("", "temp")
	}
	osMock.OpenFileF = func(name string, flag int, perm os.FileMode) (*os.File, error) {
		return ioutil.TempFile("", "temp")
	}
	osMock.ChmodF = func(name string, mode os.FileMode) error {
		return nil
	}
	osMock.WalkF = func(root string, walkFn filepath.WalkFunc) error {
		return nil
	}
	osMock.AddGroupF = func(name string) error {
		return nil
	}
	osMock.AddUserF = func(usr string, group string) error {
		return nil
	}
	osMock.LookupGroupF = func(name string) (*user.Group, error) {
		return &user.Group{}, nil
	}
	osMock.LookupF = func(username string) (*user.User, error) {
		return &user.User{}, nil
	}
}

func TestSservice_NewService(t *testing.T) {
	op := &Option{}
	ss := NewSService(op)
	assert.IsType(t, &sservice{}, ss)
}

func TestSservice_NewDaemon(t *testing.T) {
	op := Option{}
	mps := manpageService{}
	ss := sservice{osMock, &servicetest.ConfigServiceMock{}, op, mps}
	d, err := ss.NewDaemon("test", "", "")
	assert.Nil(t, err)
	dc, _ := daem.New("test", "", "")
	assert.IsType(t, d, dc)
}

func TestSservice_IsAdmin(t *testing.T) {
	op := Option{}
	mps := manpageService{}
	ss := sservice{osMock, &servicetest.ConfigServiceMock{}, op, mps}

	_, err := ss.IsAdmin()
	assert.Errorf(t, err, "you must have root user privileges. Possibly using 'sudo' command should help")
}

func TestSservice_InstallSetup_immudb(t *testing.T) {
	os.Mkdir("immutest", 0755)
	defer os.Remove("immutest")
	op := Option{}
	mps := immudbcmdtest.ManpageServiceMock{}
	ss := sservice{osMock, &servicetest.ConfigServiceMock{}, op, mps}

	err := ss.InstallSetup("immutest", &cobra.Command{})
	if err != nil {
		t.Logf("TestSservice_InstallSetup_immudb: %s", err)
	}
	assert.Nil(t, err)
}

func TestSservice_UninstallSetup_immudb(t *testing.T) {
	op := Option{}
	// provide
	op.ExecPath = "/usr/sbin/immudbnotexistentexec"
	op.ConfigPath = "/etc/immunotexistent"
	c := viper.New()
	c.Set("dir", "/var/lib/immuconfignotexistent")
	c.Set("logfile", "/var/log/immunotexist/immulognotexistent")

	mps := immudbcmdtest.ManpageServiceMock{}
	ss := sservice{osMock, c, op, mps}
	err := ss.UninstallSetup("immudb")
	assert.Nil(t, err)
}

func TestSservice_getDefaultExecPath(t *testing.T) {
	op := Option{}
	mps := manpageService{}
	ss := sservice{osMock, &servicetest.ConfigServiceMock{}, op, mps}
	path, err := ss.GetDefaultExecPath("immudb")
	assert.NotNil(t, path)
	assert.NoError(t, err)
}

func TestSservice_CopyExecInOsDefault(t *testing.T) {
	os.Mkdir("immutest", 0755)
	defer os.Remove("immutest")
	op := Option{}
	mps := manpageService{}
	ss := sservice{osMock, &servicetest.ConfigServiceMock{}, op, mps}
	_, err := ss.CopyExecInOsDefault("immutest")
	assert.Nil(t, err)
}

func TestSservice_EraseData_immudb(t *testing.T) {
	op := Option{}

	mps := manpageService{}
	c := viper.New()
	c.Set("dir", "/var/lib/immuconfignotexistent")

	ss := sservice{osMock, c, op, mps}
	err := ss.EraseData("immudb")
	assert.Nil(t, err)
}

//go:build windows
// +build windows

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

package sservice

import (
	"bytes"
	"errors"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"

	"github.com/codenotary/immudb/pkg/immuos"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	"github.com/codenotary/immudb/cmd/helper"

	"github.com/takama/daemon"
	"golang.org/x/sys/windows"
)

type sservice struct {
	os      immuos.OS
	v       ConfigService
	options Option
}

// NewSService ...
func NewSService(options *Option) *sservice {
	return &sservice{immuos.NewStandardOS(), viper.New(), *options}
}

func (ss *sservice) NewDaemon(serviceName, description string, dependencies ...string) (d daemon.Daemon, err error) {
	var ep string
	if ep, err = ss.GetDefaultExecPath(serviceName); err != nil {
		return nil, err
	}
	d, err = daemon.New(serviceName, description, ep, dependencies...)
	return d, err
}

func (ss *sservice) IsAdmin() (bool, error) {
	if runtime.GOOS == "windows" {
		var sid *windows.SID

		// Although this looks scary, it is directly copied from the
		// official windows documentation. The Go API for this is a
		// direct wrap around the official C++ API.
		// See https://docs.microsoft.com/en-us/windows/desktop/api/securitybaseapi/nf-securitybaseapi-checktokenmembership
		err := windows.AllocateAndInitializeSid(
			&windows.SECURITY_NT_AUTHORITY,
			2,
			windows.SECURITY_BUILTIN_DOMAIN_RID,
			windows.DOMAIN_ALIAS_RID_ADMINS,
			0, 0, 0, 0, 0, 0,
			&sid)
		if err != nil {
			return false, err
		}

		// This appears to cast a null pointer so I'm not sure why this
		// works, but this guy says it does and it Works for Me™:
		// https://github.com/golang/go/issues/28804#issuecomment-438838144
		token := windows.Token(0)

		_, err = token.IsMember(sid)
		// Also note that an admin is _not_ necessarily considered
		// elevated.
		// For elevation see https://github.com/mozey/run-as-admin

		if err != nil {
			return false, err
		}

		return true, nil
	}

	return false, ErrUnsupportedSystem
}

func (ss *sservice) InstallSetup(serviceName string, cmd *cobra.Command) (err error) {
	if err = ss.ReadConfig(serviceName); err != nil {
		return err
	}
	_, err = ss.CopyExecInOsDefault(serviceName)
	if err != nil {
		return err
	}
	if err = ss.InstallConfig(serviceName); err != nil {
		return err
	}
	return err
}

func (ss *sservice) UninstallSetup(serviceName string) (err error) {
	if err = ss.ReadConfig(serviceName); err != nil {
		return err
	}
	//  Program Files\{ServiceName}\{serviceName}.exe
	if err = ss.UninstallExecutables(serviceName); err != nil {
		return err
	}
	// config, pid and log in ProgramData\{ServiceName}\config
	if err = ss.RemoveProgramFiles(serviceName); err != nil {
		return err
	}
	// get immudb folder
	cepd := strings.Replace(ss.v.GetString("dir"), "data", "", 1)
	// remove immudb folder folder only if it is empty
	if _, err := os.Stat(cepd); !os.IsNotExist(err) {
		f1, err := ss.os.Open(cepd)
		if err != nil {
			return err
		}
		defer f1.Close()
		_, err = f1.Readdirnames(1)
		if err == io.EOF {
			err = ss.osRemove(cepd)
		}
	}
	return err
}

// EraseData erase all data
func (ss *sservice) EraseData(serviceName string) (err error) {
	if err = ss.ReadConfig(serviceName); err != nil {
		return err
	}
	var path string
	path = filepath.FromSlash(ss.v.GetString("dir"))
	if err := ss.osRemoveAll(path); err != nil {
		return err
	}
	return nil
}

// IsRunning check if status derives from a running process
func (ss *sservice) IsRunning(status string) bool {
	return status == "Status: SERVICE_RUNNING"
}

// GetDefaultConfigPath returns the default config path
func (ss *sservice) GetDefaultConfigPath(serviceName string) (dataDir string, err error) {
	dataDir = filepath.FromSlash(ss.v.GetString("dir"))
	var pd string
	if pd, err = windows.KnownFolderPath(windows.FOLDERID_ProgramData, windows.KF_FLAG_DEFAULT); err != nil {
		return "", err
	}
	dataDir = strings.Replace(dataDir, "%programdata%", pd, -1)
	configDir := strings.Replace(dataDir, "data", "", 1)
	return filepath.Join(strings.Title(configDir), "config", serviceName+".toml"), err
}

func (ss *sservice) ReadConfig(serviceName string) (err error) {
	ss.v.SetConfigType("toml")
	var pc string

	if pc, err = helper.ResolvePath(bytes.NewBuffer(ss.options.Config).String(), true); err != nil {
		return err
	}
	return ss.v.ReadConfig(strings.NewReader(pc))
}

func (ss *sservice) InstallConfig(serviceName string) (err error) {
	var cp string
	if err = ss.ReadConfig(serviceName); err != nil {
		return err
	}

	if cp, err = ss.GetDefaultConfigPath(serviceName); err != nil {
		return err
	}
	var configDir = filepath.Dir(cp)
	err = ss.os.MkdirAll(configDir, os.ModePerm)
	if err != nil {
		return err
	}
	return ss.v.WriteConfigAs(cp)
}

//CopyExecInOsDefault copy the executable in default exec folder and returns the path
func (ss *sservice) CopyExecInOsDefault(serviceName string) (path string, err error) {
	currentExec, err := os.Executable()
	if err != nil {
		return "", err
	}
	from, err := ss.os.Open(currentExec)
	if err != nil {
		return "", err
	}
	defer from.Close()

	path, _ = ss.GetDefaultExecPath(serviceName)
	err = ss.os.MkdirAll(filepath.Dir(path), os.ModePerm)
	if err != nil {
		return "", err
	}

	to, err := ss.os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return "", err
	}
	defer to.Close()

	if _, err = io.Copy(to, from); err != nil {
		return "", err
	}
	return path, err
}

// RemoveProgramFiles remove only config folder
func (ss *sservice) RemoveProgramFiles(serviceName string) (err error) {
	if err = ss.ReadConfig(serviceName); err != nil {
		return err
	}
	configPath, err := ss.GetDefaultConfigPath(serviceName)
	if err != nil {
		return err
	}
	return ss.osRemoveAll(filepath.Dir(configPath))
}

func (ss sservice) UninstallExecutables(serviceName string) (err error) {
	var ep string
	if ep, err = ss.GetDefaultExecPath(serviceName); err != nil {
		return err
	}
	return ss.osRemoveAll(filepath.Dir(ep))
}

// GetDefaultExecPath returns the default executable file  path
func (ss sservice) GetDefaultExecPath(serviceName string) (ep string, err error) {
	if ep, err = ss.getCommonExecPath(); err != nil {
		return "", err
	}
	return ss.os.Join(ep, strings.Title(serviceName), serviceName+".exe"), nil
}

// getCommonExecPath returns exec path for all services
func (ss *sservice) getCommonExecPath() (string, error) {
	pf, err := windows.KnownFolderPath(windows.FOLDERID_ProgramFiles, windows.KF_FLAG_DEFAULT)
	if err != nil {
		return "", err
	}
	return pf, nil
}

var whitelist = []string{"%programdata%\\Immu", "%programfile%\\Immu"}

func (ss sservice) osRemove(folder string) error {
	if err := deletionGuard(folder); err != nil {
		return err
	}
	return ss.os.Remove(folder)
}

func (ss sservice) osRemoveAll(folder string) error {
	if err := deletionGuard(folder); err != nil {
		return err
	}
	return ss.os.RemoveAll(folder)
}

func deletionGuard(path string) (err error) {
	found := false
	for _, v := range whitelist {
		var vr string
		if vr, err = helper.ResolvePath(v, false); err != nil {
			return err
		}
		if strings.HasPrefix(path, vr) {
			found = true
			break
		}
	}
	if !found {
		errors.New("os system file or folder protected item deletion not allowed. Check immu* service configuration")
	}
	return nil
}

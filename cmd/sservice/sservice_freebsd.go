//go:build freebsd
// +build freebsd

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

package sservice

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"os/user"
	"regexp"
	"strconv"
	"strings"

	"github.com/codenotary/immudb/pkg/immuos"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	"github.com/takama/daemon"
)

// NewSService ...
func NewSService(options *Option) *sservice {
	mps := NewManpageService()
	return &sservice{immuos.NewStandardOS(), viper.New(), *options, mps}
}

type sservice struct {
	os      immuos.OS
	v       ConfigService
	options Option
	mps     ManpageService
}

// NewDaemon ...
func (ss *sservice) NewDaemon(serviceName string, description string, dependencies ...string) (d daemon.Daemon, err error) {
	ep, _ := ss.GetDefaultExecPath(serviceName)
	d, err = daemon.New(serviceName, description, ep, dependencies...)
	d.SetTemplate(ss.options.StartUpConfig)
	return d, err
}

// IsAdmin check if current user is root
func (ss sservice) IsAdmin() (bool, error) {
	if output, err := exec.Command("id", "-g").Output(); err == nil {
		if gid, parseErr := strconv.ParseUint(strings.TrimSpace(string(output)), 10, 32); parseErr == nil {
			if gid == 0 {
				return true, nil
			}
			return false, ErrRootPrivileges
		}
	}
	return false, ErrUnsupportedSystem
}

// InstallSetup ...
func (ss sservice) InstallSetup(serviceName string, cmd *cobra.Command) (err error) {
	if err = ss.ReadConfig(serviceName); err != nil {
		return err
	}
	if err = ss.GroupCreateIfNotExists(); err != nil {
		return err
	}
	if err = ss.UserCreateIfNotExists(); err != nil {
		return err
	}

	execPath, err := ss.CopyExecInOsDefault(serviceName)
	if err != nil {
		return err
	}

	if err = ss.SetOwnership(execPath); err != nil {
		return err
	}

	if err = ss.InstallConfig(serviceName); err != nil {
		return err
	}

	if err = ss.os.MkdirAll(ss.v.GetString("dir"), os.ModePerm); err != nil {
		return err
	}
	if err = ss.SetOwnership(ss.v.GetString("dir")); err != nil {
		return err
	}

	logPath := ss.os.Dir(ss.v.GetString("logfile"))
	if err = ss.os.MkdirAll(logPath, os.ModePerm); err != nil {
		return err
	}
	if err = ss.SetOwnership(logPath); err != nil {
		return err
	}

	pidPath := ss.os.Dir(ss.v.GetString("pidfile"))
	if err = ss.os.MkdirAll(pidPath, os.ModePerm); err != nil {
		return err
	}
	if err = ss.SetOwnership(pidPath); err != nil {
		return err
	}

	if err = ss.InstallManPages(serviceName, cmd); err != nil {
		return err
	}

	return err
}

// UninstallSetup uninstall operations
func (ss sservice) UninstallSetup(serviceName string) (err error) {
	if err = ss.ReadConfig(serviceName); err != nil {
		return err
	}
	if err = ss.UninstallExecutables(serviceName); err != nil {
		return err
	}
	if err = ss.osRemoveAll(ss.os.Dir(ss.v.GetString("logfile"))); err != nil {
		return err
	}
	err = ss.UninstallManPages(serviceName)
	if err != nil {
		return err
	}
	// remove dir data folder only if it is empty
	cepd := ss.v.GetString("dir")
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
	cp, err := ss.GetDefaultConfigPath(serviceName)
	if err != nil {
		return err
	}
	config := ss.os.Dir(cp)
	return ss.osRemoveAll(config)
}

// installConfig install config in /etc folder
func (ss sservice) InstallConfig(serviceName string) (err error) {
	if err = ss.ReadConfig(serviceName); err != nil {
		return err
	}
	cp, _ := ss.GetDefaultConfigPath(serviceName)
	var configDir = ss.os.Dir(cp)
	err = ss.os.MkdirAll(configDir, os.ModePerm)
	if err != nil {
		return err
	}

	configPath, _ := ss.GetDefaultConfigPath(serviceName)

	if err = ss.v.WriteConfigAs(configPath); err != nil {
		return err
	}

	return ss.SetOwnership(configPath)
}

func (ss sservice) GroupCreateIfNotExists() (err error) {
	if _, err = ss.os.LookupGroup(ss.options.Group); err != user.UnknownGroupError(ss.options.Group) {
		return err
	}
	if err = ss.os.AddGroup(ss.options.Group); err != nil {
		return err
	}
	return err
}

func (ss sservice) UserCreateIfNotExists() (err error) {
	if _, err = ss.os.Lookup(ss.options.User); err != user.UnknownUserError(ss.options.User) {
		return err
	}
	if err = ss.os.AddUser(ss.options.Group, ss.options.User); err != nil {
		return err
	}

	return err
}

func (ss sservice) SetOwnership(path string) (err error) {
	var g *user.Group
	var u *user.User

	if g, err = ss.os.LookupGroup(ss.options.Group); err != nil {
		return err
	}
	if u, err = ss.os.Lookup(ss.options.User); err != nil {
		return err
	}

	uid, _ := strconv.Atoi(u.Uid)
	gid, _ := strconv.Atoi(g.Gid)

	return ss.os.Walk(path, func(name string, info os.FileInfo, err error) error {
		if err == nil {
			err = ss.osChown(name, uid, gid)
		}
		return err
	})
}

// EraseData erase all service data
func (ss sservice) EraseData(serviceName string) (err error) {
	if err = ss.ReadConfig(serviceName); err != nil {
		return err
	}
	return ss.osRemoveAll(ss.os.FromSlash(ss.v.GetString("dir")))
}

// IsRunning check if status derives from a running process
func (ss sservice) IsRunning(status string) bool {
	re := regexp.MustCompile(`is running`)
	return re.Match([]byte(status))
}

func (ss sservice) ReadConfig(serviceName string) (err error) {
	ss.v.SetConfigType("toml")
	return ss.v.ReadConfig(bytes.NewBuffer(ss.options.Config))
}

// copyExecInOsDefault copy the executable in default exec folder and returns the path. It accepts an executable absolute path
func (ss sservice) CopyExecInOsDefault(serviceName string) (string, error) {
	currentExec, err := os.Executable()
	if err != nil {
		return "", err
	}
	from, err := ss.os.Open(currentExec)
	if err != nil {
		return "", err
	}
	defer from.Close()

	path, _ := ss.GetDefaultExecPath(serviceName)

	to, err := ss.os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return "", err
	}
	defer to.Close()

	if _, err = io.Copy(to, from); err != nil {
		return "", err
	}

	if err = ss.osChmod(path, 0775); err != nil {
		return "", err
	}

	return path, err
}

func (ss sservice) GetDefaultExecPath(serviceName string) (string, error) {
	return ss.os.Join(ss.options.ExecPath, serviceName), nil
}

func (ss sservice) UninstallExecutables(serviceName string) error {
	ep, _ := ss.GetDefaultExecPath(serviceName)
	return ss.osRemove(ep)
}

func (ss sservice) InstallManPages(serviceName string, cmd *cobra.Command) error {
	if cmd != nil {
		return ss.mps.InstallManPages(ManPath, serviceName, cmd)
	}
	return nil
}

func (ss sservice) UninstallManPages(serviceName string) error {
	return ss.mps.UninstallManPages(ManPath, serviceName)
}

// GetDefaultConfigPath returns the default config path
func (ss sservice) GetDefaultConfigPath(serviceName string) (string, error) {
	return ss.os.Join(ss.options.ConfigPath, serviceName, serviceName+".toml"), nil
}

func (ss sservice) osChown(name string, uid, gid int) error {
	if err := permissionGuard(name); err != nil {
		return err
	}
	return ss.os.Chown(name, uid, gid)
}

func (ss sservice) osChmod(name string, mode os.FileMode) error {
	if err := permissionGuard(name); err != nil {
		return err
	}
	return ss.os.Chmod(name, mode)
}

var whitelist = []string{"/etc/immu", "/usr/sbin/immu", "/var/log/immu", "/var/lib/immu"}

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

func deletionGuard(path string) error {
	var v string
	found := false
	for _, v = range whitelist {
		if strings.HasPrefix(path, v) {
			found = true
			break
		}
	}
	if !found {
		return fmt.Errorf("os system file or folder protected item deletion not allowed. Check immu* service configuration: %s", path)
	}
	return nil
}

var permissionWhitelist = []string{"immu"}

func permissionGuard(path string) error {
	info, err := os.Stat(path)
	if os.IsNotExist(err) {
		return errors.New("file or folder does not exist")
	}
	if !info.IsDir() {
		// changing a specific file permissions is allowed
		return nil
	}

	found := false
	for _, v := range permissionWhitelist {
		// changing a folder permissions is allowed with restrictions
		if strings.Contains(path, v) {
			found = true
			break
		}
	}

	if !found {
		return fmt.Errorf(" Check immu* service configuration: %s", path)
	}

	return nil
}

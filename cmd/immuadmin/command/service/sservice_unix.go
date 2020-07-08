// +build linux darwin

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

package service

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"os/user"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"

	service "github.com/codenotary/immudb/cmd/immuadmin/command/service/configs"
	immudb "github.com/codenotary/immudb/cmd/immudb/command"
	immugw "github.com/codenotary/immudb/cmd/immugw/command"
	"github.com/takama/daemon"
)

const linuxExecPath = "/usr/sbin/"
const linuxConfigPath = "/etc/immudb"
const linuxManPath = "/usr/share/man/man1/"
const linuxUser = "immu"
const linuxGroup = "immu"

// NewDaemon ...
func (ss *sservice) NewDaemon(name, description, execStartPath string, dependencies ...string) (d daemon.Daemon, err error) {
	d, err = daemon.New(name, description, execStartPath, dependencies...)
	d.SetTemplate(systemDConfig)
	return d, err
}

var systemDConfig = fmt.Sprintf(`[Unit]
Description={{.Description}}
Requires={{.Dependencies}}
After={{.Dependencies}}

[Service]
PIDFile=/var/lib/immudb/{{.Name}}.pid
ExecStartPre=/bin/rm -f /var/lib/immudb/{{.Name}}.pid
ExecStart={{.Path}} {{.Args}}
Restart=on-failure
User=%s
Group=%s

[Install]
WantedBy=multi-user.target
`, linuxUser, linuxGroup)

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
func (ss sservice) InstallSetup(serviceName string) (err error) {
	if err = ss.GroupCreateIfNotExists(); err != nil {
		return err
	}
	if err = ss.UserCreateIfNotExists(); err != nil {
		return err
	}

	if err = ss.SetOwnership(linuxExecPath); err != nil {
		return err
	}

	if err = ss.InstallConfig(serviceName); err != nil {
		return err
	}

	if err = ss.oss.MkdirAll(ss.v.GetString("dir"), os.ModePerm); err != nil {
		return err
	}
	if err = ss.SetOwnership(ss.v.GetString("dir")); err != nil {
		return err
	}

	logPath := filepath.Dir(ss.v.GetString("logfile"))
	if err = os.MkdirAll(logPath, os.ModePerm); err != nil {
		return err
	}
	if err = ss.SetOwnership(logPath); err != nil {
		return err
	}

	pidPath := filepath.Dir(ss.v.GetString("pidfile"))
	if err = os.MkdirAll(pidPath, os.ModePerm); err != nil {
		return err
	}
	if err = ss.SetOwnership(pidPath); err != nil {
		return err
	}

	if err = ss.installManPages(serviceName); err != nil {
		return err
	}
	return err
}

// @todo Michele helper unix should be refactor in order to expose an interface that every service need to implemented.

// UninstallSetup uninstall operations
func (ss sservice) UninstallSetup(serviceName string) (err error) {
	if err = ss.ReadConfig(serviceName); err != nil {
		return err
	}
	if err = ss.uninstallExecutables(serviceName); err != nil {
		return err
	}
	if err = ss.uninstallManPages(serviceName); err != nil {
		return err
	}
	if err = os.RemoveAll(filepath.Dir(ss.v.GetString("logfile"))); err != nil {
		return err
	}
	return err
}

// InstallConfig install config in /etc folder
func (ss sservice) InstallConfig(serviceName string) (err error) {
	if err = ss.ReadConfig(serviceName); err != nil {
		return err
	}
	cp, _ := ss.GetDefaultConfigPath(serviceName)
	var configDir = filepath.Dir(cp)
	err = ss.oss.MkdirAll(configDir, os.ModePerm)
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
	if _, err = user.LookupGroup(linuxGroup); err != user.UnknownGroupError(linuxGroup) {
		return err
	}
	if err = exec.Command("addgroup", linuxGroup).Run(); err != nil {
		return err
	}
	return err
}

func (ss sservice) UserCreateIfNotExists() (err error) {
	if _, err = user.Lookup(linuxUser); err != user.UnknownUserError(linuxUser) {
		return err
	}
	if err = exec.Command("useradd", "-g", linuxGroup, linuxUser).Run(); err != nil {
		return err
	}

	return err
}

func (ss sservice) SetOwnership(path string) (err error) {
	var g *user.Group
	var u *user.User

	if g, err = ss.oss.LookupGroup(linuxGroup); err != nil {
		return err
	}
	if u, err = ss.oss.Lookup(linuxUser); err != nil {
		return err
	}

	uid, _ := strconv.Atoi(u.Uid)
	gid, _ := strconv.Atoi(g.Gid)

	return filepath.Walk(path, func(name string, info os.FileInfo, err error) error {
		if err == nil {
			err = ss.oss.Chown(name, uid, gid)
		}
		return err
	})
}

// todo @Michele this should be moved in UninstallSetup

// RemoveProgramFiles remove all program files
func (ss sservice) RemoveProgramFiles(serviceName string) (err error) {
	if err = ss.ReadConfig(serviceName); err != nil {
		return err
	}
	cp, err := ss.GetDefaultConfigPath(serviceName)
	if err != nil {
		return err
	}
	config := filepath.Dir(cp)
	os.RemoveAll(config)
	return
}

// EraseData erase all service data
func (ss sservice) EraseData(serviceName string) (err error) {
	if err = ss.ReadConfig(serviceName); err != nil {
		return err
	}
	return os.RemoveAll(filepath.FromSlash(ss.v.GetString("dir")))
}

// todo @Michele this can be simplified

// GetExecutable checks for the service executable name provided.
// If it's valid returns the absolute file path
// If is not valid or not presents try to use an executable presents in current executable folder.
// If found it returns the absolute file path
func (ss sservice) GetExecutable(input string, serviceName string) (exec string, err error) {
	if input == "" {
		if current, err := os.Executable(); err == nil {
			exec = filepath.Join(filepath.Dir(current), serviceName)
		} else {
			return exec, err
		}
		_, err = os.Stat(exec)
		if os.IsNotExist(err) {
			return exec, ErrExecNotFound
		}
		fmt.Printf("found an executable for the service %s on current dir\n", serviceName)
	} else {
		_, err = os.Stat(input)
		if os.IsNotExist(err) {
			return input, ErrExecNotFound
		}
		exec = input
		fmt.Printf("using provided executable for the service %s\n", serviceName)
	}
	if exec, err = filepath.Abs(exec); err != nil {
		return exec, err
	}
	return exec, err
}

// todo @Michele this can be simplified -> CopyExecInOsDefault(servicename string)
// toto @Michele use functions from the fs package?

// CopyExecInOsDefault copy the executable in default exec folder and returns the path. It accepts an executable absolute path
func (ss sservice) CopyExecInOsDefault(execPath string) (newExecPath string, err error) {
	from, err := os.Open(execPath)
	if err != nil {
		return "", err
	}
	defer from.Close()

	newExecPath, _ = ss.GetDefaultExecPath(execPath)

	to, err := os.OpenFile(newExecPath, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return "", err
	}
	defer to.Close()

	if _, err = io.Copy(to, from); err != nil {
		return "", err
	}

	if err = os.Chmod(newExecPath, 0775); err != nil {
		return "", err
	}

	return newExecPath, err
}

func (ss sservice) installManPages(serviceName string) error {
	switch serviceName {
	case "immudb":
		return immudb.InstallManPages()
	case "immugw":
		return immugw.InstallManPages(linuxManPath)
	default:
		return errors.New("invalid service name specified")
	}
}

func (ss sservice) uninstallManPages(serviceName string) error {
	switch serviceName {
	case "immudb":
		return immudb.UninstallManPages()
	case "immugw":
		return immugw.UninstallManPages(linuxManPath)
	default:
		return errors.New("invalid service name specified")
	}
}

func (ss sservice) uninstallExecutables(serviceName string) error {
	switch serviceName {
	case "immudb":
		return os.Remove(filepath.Join(linuxExecPath, "immudb"))
	case "immugw":
		return os.Remove(filepath.Join(linuxExecPath, "immugw"))
	default:
		return errors.New("invalid service name specified")
	}
}

// GetDefaultExecPath returns the default exec path. It accepts an executable or the absolute path of an executable and returns the default exec path using the exec name provided
func (ss sservice) GetDefaultExecPath(localFile string) (string, error) {
	execName := filepath.Base(localFile)
	return filepath.Join(linuxExecPath, execName), nil
}

// GetDefaultConfigPath returns the default config path
func (ss sservice) GetDefaultConfigPath(serviceName string) (string, error) {
	return filepath.Join(linuxConfigPath, serviceName+".toml"), nil
}

// IsRunning check if status derives from a running process
func (ss sservice) IsRunning(status string) bool {
	re := regexp.MustCompile(`is running`)
	return re.Match([]byte(status))
}

func (ss sservice) ReadConfig(serviceName string) (err error) {
	ss.v.SetConfigType("toml")
	return ss.v.ReadConfig(bytes.NewBuffer(configsMap[serviceName]))
}

var configsMap = map[string][]byte{
	"immudb": service.ConfigImmudb,
	"immugw": service.ConfigImmugw,
}

// UsageDet details on config and log file on specific os
var UsageDet = fmt.Sprintf(`Config file is present in %s. Log file is in /var/log/immudb`, linuxConfigPath)

// UsageExamples usage examples for linux
var UsageExamples = fmt.Sprintf(`Install the immutable database
sudo ./immuadmin service immudb install
Install the REST proxy client with rest interface. We discourage to install immugw in the same machine of immudb in order to respect the security model of our technology.
This kind of istallation is suggested only for testing purpose
sudo ./immuadmin  service immugw install
It's possible to provide a specific executable
sudo ./immuadmin  service immudb install --local-file immudb.exe
Uninstall immudb after 20 second
sudo ./immuadmin  service immudb uninstall --time 20`)

// +build linux darwin freebsd

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
	service "github.com/codenotary/immudb/cmd/immuadmin/command/service/configs"
	immudb "github.com/codenotary/immudb/cmd/immudb/command"
	immugw "github.com/codenotary/immudb/cmd/immugw/command"
	"github.com/takama/daemon"
	"io"
	"os"
	"os/exec"
	"os/user"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"

	"github.com/spf13/viper"
)

const linuxExecPath = "/usr/sbin/"
const linuxConfigPath = "/etc/immudb"
const linuxUser = "immu"
const linuxGroup = "immu"

func NewDaemon(name, description, execStartPath string, dependencies ...string) (d daemon.Daemon, err error) {
	d, err = daemon.New(name, description, execStartPath, dependencies...)
	d.SetTemplate(systemDConfig)
	return d, err
}

var systemDConfig = fmt.Sprintf(`[Unit]
Description={{.Description}}
Requires={{.Dependencies}}
After={{.Dependencies}}


[Service]
PIDFile=/var/run/{{.Name}}.pid
ExecStartPre=/bin/rm -f /var/run/{{.Name}}.pid
ExecStart={{.Path}} {{.Args}}
Restart=on-failure
User=%s
Group=%s

[Install]
WantedBy=multi-user.target
`, linuxUser, linuxGroup)

// CheckPrivileges check if current user is root
func CheckPrivileges() (bool, error) {
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

func InstallSetup(serviceName string) (err error) {
	if err = groupCreateIfNotExists(); err != nil {
		return err
	}
	if err = userCreateIfNotExists(); err != nil {
		return err
	}

	if err = setOwnership(linuxExecPath); err != nil {
		return  err
	}

	if err = installConfig(serviceName); err != nil {
		return err
	}

	if err = os.MkdirAll(viper.GetString("dir"), os.ModePerm); err != nil {
		return err
	}
	if err = setOwnership(viper.GetString("dir")); err != nil {
		return err
	}

	logPath := filepath.Dir(viper.GetString("logfile"))
	if err = os.MkdirAll(logPath, os.ModePerm); err != nil {
		return err
	}
	if err = setOwnership(logPath); err != nil {
		return err
	}

	pidPath := filepath.Dir(viper.GetString("pidfile"))
	if err = os.MkdirAll(pidPath, os.ModePerm); err != nil {
		return err
	}
	if err = setOwnership(pidPath); err != nil {
		return err
	}

	if err = installManPages(serviceName); err != nil {
		return err
	}
	return err
}

// @todo Michele helper unix should be refactor in order to expose an interface that every service need to implemented.
// UninstallSetup uninstall operations
func UninstallSetup(serviceName string) (err error) {
	if err = uninstallExecutables(serviceName); err != nil {
		return err
	}
	if err = uninstallManPages(serviceName); err != nil {
		return err
	}
	if err = os.RemoveAll(filepath.Dir(viper.GetString("logfile"))); err != nil {
		return err
	}
	return err
}

// installConfig install config in /etc folder
func installConfig(serviceName string) (err error) {
	if err = readConfig(serviceName); err != nil {
		return err
	}
	var configDir = filepath.Dir(GetDefaultConfigPath(serviceName))
	err = os.MkdirAll(configDir, os.ModePerm)
	if err != nil {
		return err
	}

	configPath := GetDefaultConfigPath(serviceName)

	if err = viper.WriteConfigAs(configPath); err != nil {
		return err
	}

	return setOwnership(configPath)
}

func groupCreateIfNotExists() (err error) {
	if _, err = user.LookupGroup(linuxGroup); err != user.UnknownGroupError(linuxGroup) {
		return err
	}
	if err = exec.Command("addgroup", linuxGroup).Run(); err != nil {
		return err
	}
	return err
}

func userCreateIfNotExists() (err error) {
	if _, err = user.Lookup(linuxUser); err != user.UnknownUserError(linuxUser) {
		return err
	}
	if err = exec.Command("useradd", "-g", linuxGroup, linuxUser).Run(); err != nil {
		return err
	}

	return err
}

func setOwnership(path string) (err error) {
	var g *user.Group
	var u *user.User

	if g, err = user.LookupGroup(linuxGroup); err != nil {
		return err
	}
	if u, err = user.Lookup(linuxUser); err != nil {
		return err
	}

	uid, _ := strconv.Atoi(u.Uid)
	gid, _ := strconv.Atoi(g.Gid)

	return filepath.Walk(path, func(name string, info os.FileInfo, err error) error {
		if err == nil {
			err = os.Chown(name, uid, gid)
		}
		return err
	})
}

// todo @Michele this should be moved in UninstallSetup
// RemoveProgramFiles remove all program files
func RemoveProgramFiles(serviceName string) (err error) {
	if err = readConfig(serviceName); err != nil {
		return err
	}
	config := filepath.Dir(GetDefaultConfigPath(serviceName))
	os.RemoveAll(config)
	return
}

// EraseData erase all service data
func EraseData(serviceName string) (err error) {
	if err = readConfig(serviceName); err != nil {
		return err
	}
	return os.RemoveAll(filepath.FromSlash(viper.GetString("dir")))
}

// todo @Michele this can be simplified
// GetExecutable checks for the service executable name provided.
// If it's valid returns the absolute file path
// If is not valid or not presents try to use an executable presents in current executable folder.
// If found it returns the absolute file path
func GetExecutable(input string, serviceName string) (exec string, err error) {
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
//CopyExecInOsDefault copy the executable in default exec folder and returns the path. It accepts an executable absolute path
func CopyExecInOsDefault(execPath string) (newExecPath string, err error) {
	from, err := os.Open(execPath)
	if err != nil {
		return "", err
	}
	defer from.Close()

	newExecPath = GetDefaultExecPath(execPath)

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

func installManPages(serviceName string) error {
	switch serviceName {
	case "immudb":
		return immudb.InstallManPages()
	case "immugw":
		return immugw.InstallManPages()
	default:
		return errors.New("invalid service name specified")
	}
}

func uninstallManPages(serviceName string) error {
	switch serviceName {
	case "immudb":
		return immudb.UnistallManPages()
	case "immugw":
		return immugw.UnistallManPages()
	default:
		return errors.New("invalid service name specified")
	}
}

func uninstallExecutables(serviceName string) error {
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
func GetDefaultExecPath(localFile string) string {
	execName := filepath.Base(localFile)
	return filepath.Join(linuxExecPath, execName)
}

// GetDefaultConfigPath returns the default config path
func GetDefaultConfigPath(serviceName string) string {
	return filepath.Join(linuxConfigPath, serviceName+".toml")
}

// IsRunning check if status derives from a running process
func IsRunning(status string) bool {
	re := regexp.MustCompile(`is running`)
	return re.Match([]byte(status))
}

func readConfig(serviceName string) (err error) {
	viper.SetConfigType("toml")
	return viper.ReadConfig(bytes.NewBuffer(configsMap[serviceName]))
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

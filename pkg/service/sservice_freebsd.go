// +build freebsd

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
	"regexp"
	"strconv"
	"strings"

	"github.com/takama/daemon"
)

func (ss *sservice) NewDaemon(name, description, execStartPath string, dependencies ...string) (d daemon.Daemon, err error) {
	d, err = daemon.New(name, description, execStartPath, dependencies...)
	return d, err
}

// IsAdmin check if current user is root
func (ss *sservice) IsAdmin() (bool, error) {
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

func (ss *sservice) InstallSetup(serviceName string) (err error) {
	if err = ss.groupCreateIfNotExists(); err != nil {
		return err
	}
	if err = ss.UserCreateIfNotExists(); err != nil {
		return err
	}
	if err = ss.SetOwnership(ss.options.ExecPath); err != nil {
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
	if err = ss.enableStartOnBoot(serviceName); err != nil {
		return err
	}
	if ss.options.ManPath == "" {
		if err = ss.installManPages(serviceName); err != nil {
			return err
		}
	}
	return err
}

func (ss *sservice) enableStartOnBoot(serviceName string) error {
	f, err := ss.os.OpenFile("/etc/rc.conf",
		os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer f.Close()
	cmd := fmt.Sprintf(`%s_enable="YES"%s`, serviceName, "\n")
	if _, err := f.WriteString(cmd); err != nil {
		return err
	}
	return nil
}

// @todo Michele helper unix should be refactor in order to expose an interface that every service need to implemented.
// UninstallSetup uninstall operations
func (ss *sservice) UninstallSetup(serviceName string) (err error) {
	if err = ss.ReadConfig(serviceName); err != nil {
		return err
	}
	if err = ss.uninstallExecutables(serviceName); err != nil {
		return err
	}
	if ss.options.ManPath == "" {
		if err = ss.uninstallManPages(serviceName); err != nil {
			return err
		}
	}
	if err = ss.os.RemoveAll(ss.os.Dir(ss.v.GetString("logfile"))); err != nil {
		return err
	}
	return err
}

// InstallConfig install config in /etc folder
func (ss *sservice) InstallConfig(serviceName string) (err error) {
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

func (ss *sservice) groupCreateIfNotExists() (err error) {
	if _, err = user.LookupGroup(ss.options.Group); err != user.UnknownGroupError(ss.options.Group) {
		return err
	}
	if err = exec.Command("pw", "groupadd", ss.options.Group).Run(); err != nil {
		return err
	}
	return err
}

func (ss *sservice) UserCreateIfNotExists() (err error) {
	if _, err = user.Lookup(ss.options.User); err != user.UnknownUserError(ss.options.User) {
		return err
	}
	if err = exec.Command("pw", "useradd", ss.options.User, "-G", ss.options.Group).Run(); err != nil {
		return err
	}
	return err
}

func (ss *sservice) SetOwnership(path string) (err error) {
	var g *user.Group
	var u *user.User

	if g, err = user.LookupGroup(ss.options.Group); err != nil {
		return err
	}
	if u, err = user.Lookup(ss.options.User); err != nil {
		return err
	}

	uid, _ := strconv.Atoi(u.Uid)
	gid, _ := strconv.Atoi(g.Gid)

	return ss.os.Walk(path, func(name string, info os.FileInfo, err error) error {
		if err == nil {
			err = ss.os.Chown(name, uid, gid)
		}
		return err
	})
}

// todo @Michele this should be moved in UninstallSetup
// RemoveProgramFiles remove all program files
func (ss *sservice) RemoveProgramFiles(serviceName string) (err error) {
	if err = ss.ReadConfig(serviceName); err != nil {
		return err
	}
	cp, err := ss.GetDefaultConfigPath(serviceName)
	if err != nil {
		return err
	}
	config := ss.os.Dir(cp)
	ss.os.RemoveAll(config)
	return
}

// EraseData erase all service data
func (ss *sservice) EraseData(serviceName string) (err error) {
	if err = ss.ReadConfig(serviceName); err != nil {
		return err
	}
	return ss.os.RemoveAll(ss.os.FromSlash(ss.v.GetString("dir")))
}

// todo @Michele this can be simplified
// GetExecutable checks for the service executable name provided.
// If it's valid returns the absolute file path
// If is not valid or not presents try to use an executable presents in current executable folder.
// If found it returns the absolute file path
func (ss *sservice) GetExecutable(input string, serviceName string) (exec string, err error) {
	if input == "" {
		if current, err := os.Executable(); err == nil {
			exec = ss.os.Join(ss.os.Dir(current), serviceName)
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
	if exec, err = ss.os.Abs(exec); err != nil {
		return exec, err
	}
	return exec, err
}

// todo @Michele this can be simplified -> CopyExecInOsDefault(servicename string)
// toto @Michele use functions from the fs package?
//CopyExecInOsDefault copy the executable in default exec folder and returns the path. It accepts an executable absolute path
func (ss *sservice) CopyExecInOsDefault(execPath string) (newExecPath string, err error) {
	from, err := ss.os.Open(execPath)
	if err != nil {
		return "", err
	}
	defer from.Close()

	newExecPath, _ = ss.GetDefaultExecPath(execPath)
	fmt.Println("new exec path", newExecPath)
	to, err := ss.os.OpenFile(newExecPath, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return "", err
	}
	defer to.Close()

	if _, err = io.Copy(to, from); err != nil {
		return "", err
	}

	if err = ss.os.Chmod(newExecPath, 0775); err != nil {
		return "", err
	}

	return newExecPath, err
}

func (ss sservice) installManPages(serviceName string) error {
	switch serviceName {
	case "immudb":
		return ss.mpss[0].InstallManPages(ss.options.ManPath)
	case "immugw":
		return ss.mpss[1].InstallManPages(ss.options.ManPath)
	default:
		return errors.New("invalid service name specified")
	}
}

func (ss sservice) uninstallManPages(serviceName string) error {
	switch serviceName {
	case "immudb":
		return ss.mpss[0].UninstallManPages(ss.options.ManPath)
	case "immugw":
		return ss.mpss[1].UninstallManPages(ss.options.ManPath)
	default:
		return errors.New("invalid service name specified")
	}
}

func (ss *sservice) uninstallExecutables(serviceName string) error {
	switch serviceName {
	case "immudb":
		return ss.os.Remove(ss.os.Join(ss.options.ExecPath, "immudb"))
	case "immugw":
		return ss.os.Remove(ss.os.Join(ss.options.ExecPath, "immugw"))
	case "immuclient":
		return ss.os.Remove(ss.os.Join(ss.options.ExecPath, "immuclient"))
	default:
		return errors.New("invalid service name specified")
	}
}

// GetDefaultExecPath returns the default exec path. It accepts an executable or the absolute path of an executable and returns the default exec path using the exec name provided
func (ss *sservice) GetDefaultExecPath(localFile string) (string, error) {
	execName := ss.os.Base(localFile)
	return ss.os.Join(ss.options.ExecPath, execName), nil
}

// GetDefaultConfigPath returns the default config path
func (ss *sservice) GetDefaultConfigPath(serviceName string) (string, error) {
	return ss.os.Join(ss.options.ConfigPath, serviceName+".toml"), nil
}

// IsRunning check if status derives from a running process
func (ss *sservice) IsRunning(status string) bool {
	re := regexp.MustCompile(`is running`)
	return re.Match([]byte(status))
}

func (ss *sservice) ReadConfig(serviceName string) (err error) {
	ss.v.SetConfigType("toml")
	return ss.v.ReadConfig(bytes.NewBuffer([]byte(ss.options.Config[serviceName])))
}

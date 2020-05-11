// +build windows

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
	"fmt"
	service "github.com/codenotary/immudb/cmd/immuadmin/command/service/configs"
	"github.com/spf13/viper"
	"github.com/takama/daemon"
	"golang.org/x/sys/windows"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
)

const winExecPath = "C:\\Windows\\System32\\"

func NewDaemon(name, description, execStartPath string, dependencies ...string) (d daemon.Daemon, err error) {
	d, err = daemon.New(name, description, execStartPath, dependencies...)
	return d, err
}

func CheckPrivileges() (bool, error) {
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

func InstallSetup(serviceName string) (err error) {
	if err = installConfig(serviceName); err != nil {
		return err
	}
	return err
}

func UninstallSetup(serviceName string) (err error) {
	// Useless at the moment
	return err
}

func installConfig(serviceName string) (err error) {
	if err = readConfig(serviceName); err != nil {
		return err
	}
	var configDir = filepath.Dir(GetDefaultConfigPath(serviceName))
	err = os.MkdirAll(configDir, os.ModePerm)
	if err != nil {
		return err
	}
	return viper.WriteConfigAs(GetDefaultConfigPath(serviceName))
}

// RemoveProgramFiles remove all program files
func RemoveProgramFiles(serviceName string) (err error) {
	if err = readConfig(serviceName); err != nil {
		return err
	}
	return os.RemoveAll(filepath.Join(filepath.FromSlash(viper.GetString("dir")), "config"))
}

// EraseData erase all data
func EraseData(serviceName string) (err error) {
	if err = readConfig(serviceName); err != nil {
		return err
	}
	return os.RemoveAll(filepath.FromSlash(viper.GetString("dir")))
}

// GetExecutable looks for the service executable name provided or try to use an executable presents in current folder. It returns the absolute file path
func GetExecutable(input string, serviceName string) (exec string, err error) {
	if input == "" {
		exec = serviceName
		exec = exec + ".exe"
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

//CopyExecInOsDefault copy the executable in default exec folder and returns the path
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
	return newExecPath, err
}

// GetDefaultExecPath returns the default exec path
func GetDefaultExecPath(execPath string) string {
	execName := filepath.Base(execPath)
	return filepath.Join(winExecPath, execName)
}

// GetDefaultConfigPath returns the default config path
func GetDefaultConfigPath(serviceName string) string {
	var dataDir = filepath.FromSlash(viper.GetString("dir"))
	return filepath.Join(strings.Title(dataDir), "config", serviceName+".toml")
}

// IsRunning check if status derives from a running process
func IsRunning(status string) bool {
	return status == "Status: SERVICE_RUNNING"
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
var UsageDet = fmt.Sprintf(`Config and log files are present in C:\ProgramData\Immudb folder`)

// UsageExamples examples
var UsageExamples = fmt.Sprintf(`Install the immutable database
immuadmin.exe service immudb install
Install the REST proxy client with rest interface. We discourage to install immugw in the same machine of immudb in order to respect the security model of our technology.
This kind of istallation is suggested only for testing purpose
immuadmin.exe service immugw install
It's possible to provide a specific executable
immuadmin.exe service immudb install --local-file immudb.exe
Uninstall immudb after 20 second
immuadmin.exe service immudb uninstall --time 20
`)

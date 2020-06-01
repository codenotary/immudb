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
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"

	"github.com/codenotary/immudb/cmd/helper"

	service "github.com/codenotary/immudb/cmd/immuadmin/command/service/configs"
	"github.com/spf13/viper"
	"github.com/takama/daemon"
	"golang.org/x/sys/windows"
)

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
	// remove ProgramFiles folder only if it is empty
	var cep string
	if cep, err = getCommonExecPath(); err != nil {
		return err
	}
	err = os.Remove(filepath.Join(cep, serviceName+".exe"))
	if err != nil {
		return err
	}
	f, err := os.Open(cep)
	if err != nil {
		return err
	}
	defer f.Close()
	_, err = f.Readdirnames(1)
	if err == io.EOF {
		err = os.Remove(cep)
	}
	// remove ProgramData folder only if it is empty
	var cepd string
	if cepd, err = helper.ResolvePath(viper.GetString("dir"), false); err != nil {
		return err
	}
	f1, err := os.Open(cepd)
	if err != nil {
		return err
	}
	defer f1.Close()
	_, err = f1.Readdirnames(1)
	if err == io.EOF {
		err = os.Remove(cepd)
	}
	return err
}

func installConfig(serviceName string) (err error) {
	var cp string
	if err = readConfig(serviceName); err != nil {
		return err
	}

	if cp, err = GetDefaultConfigPath(serviceName); err != nil {
		return err
	}
	var configDir = filepath.Dir(cp)
	err = os.MkdirAll(configDir, os.ModePerm)
	if err != nil {
		return err
	}
	return viper.WriteConfigAs(cp)
}

// RemoveProgramFiles remove all program files
func RemoveProgramFiles(serviceName string) (err error) {
	var path string
	if err = readConfig(serviceName); err != nil {
		return err
	}
	if path, err = helper.ResolvePath(filepath.Join(filepath.FromSlash(viper.GetString("dir")), "config"), false); err != nil {
		return err
	}
	return os.RemoveAll(path)
}

// EraseData erase all data
func EraseData(serviceName string) (err error) {
	if err = readConfig(serviceName); err != nil {
		return err
	}
	var path string
	if path, err = helper.ResolvePath(filepath.FromSlash(viper.GetString("dir")), false); err != nil {
		return err
	}
	data := filepath.Join(path, "data")
	if err := os.RemoveAll(data); err != nil {
		return err
	}
	immudbsys := filepath.Join(path, "immudbsys")
	if err := os.RemoveAll(immudbsys); err != nil {
		return err
	}
	if err := os.RemoveAll(filepath.Join(path, "immudb.identifier")); err != nil {
		return err
	}
	return nil
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

// todo @Michele use functions from the fs package?
//CopyExecInOsDefault copy the executable in default exec folder and returns the path
func CopyExecInOsDefault(execPath string) (newExecPath string, err error) {
	// exec path folder install creation
	// todo @Michele this should be move in installSetup
	var cep string
	if cep, err = getCommonExecPath(); err != nil {
		return "", err
	}
	err = os.MkdirAll(cep, os.ModePerm)
	if err != nil {
		return "", err
	}

	from, err := os.Open(execPath)
	if err != nil {
		return "", err
	}
	defer from.Close()

	newExecPath, err = GetDefaultExecPath(execPath)
	if err != nil {
		return "", err
	}
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
func GetDefaultExecPath(execPath string) (string, error) {
	execName := filepath.Base(execPath)
	cp, err := getCommonExecPath()
	if err != nil {
		return "", err
	}
	return filepath.Join(cp, execName), nil
}

// getCommonExecPath returns exec path for all services
func getCommonExecPath() (string, error) {
	pf, err := windows.KnownFolderPath(windows.FOLDERID_ProgramFiles, windows.KF_FLAG_DEFAULT)
	if err != nil {
		return "", err
	}
	return filepath.Join(pf, "Immudb"), nil
}

// GetDefaultConfigPath returns the default config path
func GetDefaultConfigPath(serviceName string) (dataDir string, err error) {
	dataDir = filepath.FromSlash(viper.GetString("dir"))
	var pd string
	if pd, err = windows.KnownFolderPath(windows.FOLDERID_ProgramData, windows.KF_FLAG_DEFAULT); err != nil {
		return "", err
	}
	dataDir = strings.Replace(dataDir, "%programdata%", pd, -1)
	return filepath.Join(strings.Title(dataDir), "config", serviceName+".toml"), err
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

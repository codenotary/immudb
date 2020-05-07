// +build linux

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
	service "github.com/codenotary/immudb/cmd/immuadmin/service/configs"
	"github.com/spf13/viper"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
)

const linuxExecPath = "/usr/sbin/"
const linuxConfigPath = "/etc/immudb"

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

// InstallConfig install config in /etc folder
func InstallConfig(serviceName string) (err error) {
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
	config := filepath.Dir(GetDefaultConfigPath(serviceName))
	os.RemoveAll(config)
	return
}

// EraseData erase all service data
func EraseData(serviceName string) (err error) {
	if err = readConfig(serviceName); err != nil {
		return err
	}
	return os.RemoveAll(filepath.FromSlash(viper.GetString("default.dir")))
}

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

	os.Chmod(newExecPath, 0775)
	return newExecPath, err
}

// GetDefaultExecPath returns the default exec path. It accepts an executable or the absolute path of an executable and returns the default exec path using the exec name provided
func GetDefaultExecPath(localFile string) string {
	execName := filepath.Base(localFile)
	return filepath.Join(linuxExecPath, execName)
}

// GetDefaultConfigPath returns the default config path
func GetDefaultConfigPath(serviceName string) string {
	return filepath.Join(linuxConfigPath, serviceName+".ini")
}

// IsRunning check if status derives from a running process
func IsRunning(status string) bool {
	re := regexp.MustCompile(`is running`)
	return re.Match([]byte(status))
}

func readConfig(serviceName string) (err error) {
	viper.SetConfigType("ini")
	return viper.ReadConfig(bytes.NewBuffer(configsMap[serviceName]))
}

var configsMap = map[string][]byte{
	"immudb": service.ConfigImmudb,
	"immugw": service.ConfigImmugw,
}

// UsageDet details on config and log file on specific os
var UsageDet = fmt.Sprintf(`Config file is present in %s. Log file is in /var/log/immudb`, linuxConfigPath)
var UsageExamples = fmt.Sprintf(`Install the immutable database
sudo ./immuadmin service immudb install
Install the REST proxy client with rest interface. We discourage to install immugw in the same machine of immudb in order to respect the security model of our technology.
This kind of istallation is suggested only for testing purpose
sudo ./immuadmin  service immugw install
It's possible to provide a specific executable
sudo ./immuadmin  service immudb install --local-file immudb.exe
Uninstall immudb after 20 second
sudo ./immuadmin  service immudb uninstall --time 20`)

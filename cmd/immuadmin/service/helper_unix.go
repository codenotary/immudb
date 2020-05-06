// +build unix

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
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
)

func CheckPrivileges() (bool, error) {
	if runtime.GOOS != "windows" {
		if output, err := exec.Command("id", "-g").Output(); err == nil {
			if gid, parseErr := strconv.ParseUint(strings.TrimSpace(string(output)), 10, 32); parseErr == nil {
				if gid == 0 {
					return true, nil
				}
				return false, ErrRootPrivileges
			}
		}
	}
	return false, ErrUnsupportedSystem
}

func InstallConfig(serviceName string) error {
	err := os.MkdirAll("/etc/"+serviceName, os.ModePerm)
	if err != nil {
		return err
	}

	from, err := os.Open("configs/immudb.ini.dist")
	if err != nil {
		return err
	}
	defer from.Close()

	to, err := os.OpenFile("/etc/"+serviceName+"/"+serviceName+".ini", os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return err
	}
	defer to.Close()

	_, err = io.Copy(to, from)
	if err != nil {
		return err
	}
	return nil
}

func UninstallConfig(serviceName string) error {
	err := os.Remove("/etc/" + serviceName + "/" + serviceName + ".ini")
	if err != nil {
		return err
	}
	return nil
}

func GetExecutable(input string, serviceName string) (exec string, err error) {
	if input != "" {
		_, err = os.Stat(input)
		if os.IsNotExist(err) {
			return exec, ErrExecNotFound
		}
	}

	if input == "" {
		exec = serviceName
		_, err = os.Stat(exec)
		if os.IsNotExist(err) {
			return exec, ErrExecNotFound
		}
		input = exec
	}
	if exec, err = filepath.Abs(exec); err != nil {
		return exec, err
	}
	return exec, err
}

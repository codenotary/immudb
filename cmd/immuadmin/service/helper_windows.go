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
	"golang.org/x/sys/windows"
	"runtime"
)

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
	if input == "" {
		exec = serviceName
		exec = exec + ".exe"
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

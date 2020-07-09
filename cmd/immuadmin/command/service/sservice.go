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
	immudb "github.com/codenotary/immudb/cmd/immudb/command"
	immugw "github.com/codenotary/immudb/cmd/immugw/command"
	"github.com/spf13/viper"
	"github.com/takama/daemon"
	"io"
)

type Sservice interface {
	NewDaemon(name, description, execStartPath string, dependencies ...string) (d daemon.Daemon, err error)
	IsAdmin() (bool, error)
	InstallSetup(serviceName string) (err error)
	UninstallSetup(serviceName string) (err error)
	InstallConfig(serviceName string) (err error)
	RemoveProgramFiles(serviceName string) (err error)
	EraseData(serviceName string) (err error)
	GetExecutable(input string, serviceName string) (exec string, err error)
	CopyExecInOsDefault(execPath string) (newExecPath string, err error)
	GetDefaultExecPath(localFile string) (string, error)
	GetDefaultConfigPath(serviceName string) (string, error)
	IsRunning(status string) bool
	ReadConfig(serviceName string) (err error)
}

type SserviceManPages interface {
	installManPages(serviceName string) error
	uninstallManPages(serviceName string) error
}

type SservicePermissions interface {
	GroupCreateIfNotExists() (err error)
	UserCreateIfNotExists() (err error)
	SetOwnership(path string) (err error)
}

type SserviceTOREFACTOR interface {
	uninstallExecutables(serviceName string) error
}

func NewSService() *sservice {
	mpss := make([]immudb.ManpageService, 2)
	mpss[0] = immudb.ManpageServiceImmudb{}
	mpss[1] = immugw.ManpageServiceImmugw{}
	return &sservice{oss{}, filepaths{}, viper.New(), mpss}
}

type ConfigService interface {
	WriteConfigAs(filename string) error
	GetString(key string) string
	SetConfigType(in string)
	ReadConfig(in io.Reader) error
}

type sservice struct {
	oss  Oss
	fps  Filepaths
	v    ConfigService
	mpss []immudb.ManpageService
}

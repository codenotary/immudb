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

	immudb "github.com/codenotary/immudb/cmd/immudb/command"
	immugw "github.com/codenotary/immudb/cmd/immugw/command"
	"github.com/codenotary/immudb/pkg/immuos"
	"github.com/spf13/viper"
	"github.com/takama/daemon"
)

// Sservice ...
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

// SserviceManPages ...
type SserviceManPages interface {
	installManPages(serviceName string) error
	uninstallManPages(serviceName string) error
}

// SservicePermissions ...
type SservicePermissions interface {
	GroupCreateIfNotExists() (err error)
	UserCreateIfNotExists() (err error)
	SetOwnership(path string) (err error)
}

// SserviceTOREFACTOR ...
type SserviceTOREFACTOR interface {
	uninstallExecutables(serviceName string) error
}

// NewSService ...
func NewSService(options *Option) *sservice {
	mpss := make([]immudb.ManpageService, 2)
	mpss[0] = immudb.ManpageServiceImmudb{}
	mpss[1] = immugw.ManpageServiceImmugw{}
	return &sservice{immuos.NewStandardOS(), viper.New(), mpss, *options}
}

// ConfigService ...
type ConfigService interface {
	WriteConfigAs(filename string) error
	GetString(key string) string
	SetConfigType(in string)
	ReadConfig(in io.Reader) error
}

type sservice struct {
	os      immuos.OS
	v       ConfigService
	mpss    []immudb.ManpageService
	options Option
}

/*
Copyright 2025 Codenotary Inc. All rights reserved.

SPDX-License-Identifier: BUSL-1.1
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    https://mariadb.com/bsl11/

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package sservice

import (
	"io"

	"github.com/spf13/cobra"
	"github.com/takama/daemon"
)

// Sservice ...
type Sservice interface {
	NewDaemon(serviceName, description string, dependencies ...string) (d daemon.Daemon, err error)
	IsAdmin() (bool, error)
	InstallSetup(serviceName string, cmd *cobra.Command) (err error)
	UninstallSetup(serviceName string) (err error)
	EraseData(serviceName string) (err error)
	IsRunning(status string) bool
	GetDefaultConfigPath(serviceName string) (string, error)

	ReadConfig(serviceName string) (err error)
	InstallConfig(serviceName string) (err error)
	CopyExecInOsDefault(execPath string) (newExecPath string, err error)
	GetDefaultExecPath(serviceName string) (string, error)
	UninstallExecutables(serviceName string) error
}

// SservicePermissions ...
type SservicePermissions interface {
	GroupCreateIfNotExists() (err error)
	UserCreateIfNotExists() (err error)
	SetOwnership(path string) (err error)
}

// ConfigService ...
type ConfigService interface {
	WriteConfigAs(filename string) error
	GetString(key string) string
	SetConfigType(in string)
	ReadConfig(in io.Reader) error
}

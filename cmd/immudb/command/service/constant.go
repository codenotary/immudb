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

package service

import "errors"

var (
	// ErrUnsupportedSystem appears if try to use service on system which is not supported by this release
	ErrUnsupportedSystem = errors.New("unsupported system")

	// ErrRootPrivileges appears if run installation or deleting the service without root privileges
	ErrRootPrivileges = errors.New("you must have root user privileges. Possibly using 'sudo' command should help")

	// ErrExecNotFound provided executable file does not exists
	ErrExecNotFound = errors.New("executable file does not exists or not provided")

	// ErrServiceNotInstalled provided executable file does not exists
	ErrServiceNotInstalled = errors.New("Service is not installed")
)

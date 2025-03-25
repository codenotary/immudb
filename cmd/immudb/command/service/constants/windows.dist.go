//go:build windows
// +build windows

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

package constants

import "fmt"

const ExecPath = ""
const ConfigPath = ""
const OSUser = ""
const OSGroup = ""

var StartUpConfig = ""

// UsageDet details on config and log file on specific os
var UsageDet = fmt.Sprintf(`Config and log files are present in C:\ProgramData\Immudb folder`)

// UsageExamples usage examples for linux
var UsageExamples = fmt.Sprintf(`Install the immutable database
immudb.exe  service install    -  Initializes and runs daemon
immudb.exe  service stop       -  Stops the daemon
immudb.exe  service start      -  Starts initialized daemon
immudb.exe  service restart    -  Restarts daemon
immudb.exe  service uninstall  -  Removes daemon and its setup
Uninstall immudb after 20 second
immudb.exe  service uninstall --time 20 immudb.exe`)

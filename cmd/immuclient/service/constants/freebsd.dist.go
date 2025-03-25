//go:build freebsd
// +build freebsd

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

const ExecPath = "/usr/sbin/"
const ConfigPath = "/etc/immudb"
const ManPath = ""
const OSUser = "immu"
const OSGroup = "immu"

var StartUpConfig = ""

// UsageDet details on config and log file on specific os
var UsageDet = fmt.Sprintf(`Config file is present in %s. Log file is in /var/log/immuclient`, ConfigPath)

// UsageExamples usage examples for linux
var UsageExamples = `
Install immudb immutable database and immuclient
immuclient audit-mode install    -  Initializes and runs daemon
immuclient audit-mode stop       -  Stops the daemon
immuclient audit-mode start      -  Starts initialized daemon
immuclient audit-mode restart    -  Restarts daemon
immuclient audit-mode uninstall  -  Removes daemon and its setup
`

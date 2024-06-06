//go:build linux || darwin
// +build linux darwin

/*
Copyright 2024 Codenotary Inc. All rights reserved.

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
const ConfigPath = "/etc/"
const OSUser = "immu"
const OSGroup = "immu"

var StartUpConfig = fmt.Sprintf(`[Unit]
Description={{.Description}}
Requires={{.Dependencies}}
After={{.Dependencies}}
[Service]
PIDFile=/var/lib/immuclient/{{.Name}}.pid
ExecStartPre=/bin/rm -f /var/lib/immuclient/{{.Name}}.pid
ExecStart={{.Path}} {{.Args}}
Restart=on-failure
User=%s
Group=%s
[Install]
WantedBy=multi-user.target
`, OSUser, OSGroup)

// UsageDet details on config and log file on specific os
var UsageDet = fmt.Sprintf(`Config file is present in %s. Log file is in /var/log/immuclient`, ConfigPath)

// UsageExamples usage examples for linux
var UsageExamples = `
Install immudb immutable database and immuclient
immuclient audit-mode            -  Run a foreground auditor
immuclient audit-mode install    -  Install and runs daemon
immuclient audit-mode stop       -  Stops the daemon
immuclient audit-mode start      -  Starts initialized daemon
immuclient audit-mode restart    -  Restarts daemon
immuclient audit-mode uninstall  -  Removes daemon and its setup
`

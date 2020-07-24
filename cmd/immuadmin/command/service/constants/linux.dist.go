// +build linux darwin

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

import "fmt"

const ExecPath = "/usr/sbin/"
const ConfigPath = "/etc/immudb"
const ManPath = "/usr/share/man/man1/"
const OSUser = "immu"
const OSGroup = "immu"

var StartUpConfig = fmt.Sprintf(`[Unit]
Description={{.Description}}
Requires={{.Dependencies}}
After={{.Dependencies}}

[Service]
PIDFile=/var/lib/immudb/{{.Name}}.pid
ExecStartPre=/bin/rm -f /var/lib/immudb/{{.Name}}.pid
ExecStart={{.Path}} {{.Args}}
Restart=on-failure
User=%s
Group=%s

[Install]
WantedBy=multi-user.target
`, OSUser, OSGroup)

// UsageDet details on config and log file on specific os
var UsageDet = fmt.Sprintf(`Config file is present in %s. Log file is in /var/log/immudb`, ConfigPath)

// UsageExamples usage examples for linux
var UsageExamples = fmt.Sprintf(`Install the immutable database
sudo ./immuadmin service immudb install
Install the REST proxy client with rest interface. We discourage to install immugw in the same machine of immudb in order to respect the security model of our technology.
This kind of istallation is suggested only for testing purpose
sudo ./immuadmin  service immugw install
It's possible to provide a specific executable
sudo ./immuadmin  service immudb install --local-file immudb.exe
Uninstall immudb after 20 second
sudo ./immuadmin  service immudb uninstall --time 20`)

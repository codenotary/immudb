// +build windows

/*
Copyright 2022 CodeNotary, Inc. All rights reserved.

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

package constants

import "fmt"

const ExecPath = ""
const ConfigPath = ""
const OSUser = ""
const OSGroup = ""

var StartUpConfig = ""

// UsageDet details on config and log file on specific os
var UsageDet = fmt.Sprintf(`Config and log files are present in C:\ProgramData\Immudb folder`)

// UsageExamples examples
var UsageExamples = `
Install immudb auditor
immuclient.exe audit-mode install    -  Initializes and runs daemon
immuclient.exe audit-mode stop       -  Stops the daemon
immuclient.exe audit-mode start      -  Starts initialized daemon
immuclient.exe audit-mode restart    -  Restarts daemon
immuclient.exe audit-mode uninstall  -  Removes daemon and its setup
`

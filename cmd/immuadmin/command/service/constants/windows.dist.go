// +build windows

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

const ExecPath = ""
const ConfigPath = ""
const ManPath = ""
const OSUser = ""
const OSGroup = ""

var StartUpConfig = ""

// UsageDet details on config and log file on specific os
var UsageDet = fmt.Sprintf(`Config and log files are present in C:\ProgramData\Immudb folder`)

// UsageExamples examples
var UsageExamples = fmt.Sprintf(`Install the immutable database
immuadmin.exe service immudb install
Install the REST proxy client with rest interface. We discourage to install immugw in the same machine of immudb in order to respect the security model of our technology.
This kind of istallation is suggested only for testing purpose
immuadmin.exe service immugw install
It's possible to provide a specific executable
immuadmin.exe service immudb install --local-file immudb.exe
Uninstall immudb after 20 second
immuadmin.exe service immudb uninstall --time 20
`)

// +build freebsd

/*
Copyright 2021 CodeNotary, Inc. All rights reserved.

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

const ExecPath = "/usr/sbin/"
const ConfigPath = "/etc/"
const OSUser = "immu"
const OSGroup = "immu"

var StartUpConfig = ""

// UsageDet details on config and log file on specific os
var UsageDet = fmt.Sprintf(`Config file is present in %s. Log file is in /var/log/immudb`, ConfigPath)

// UsageExamples usage examples for linux
var UsageExamples = fmt.Sprintf(`Install the immutable database
sudo ./immudb service install    -  Installs the daemon
sudo ./immudb service stop       -  Stops the daemon
sudo ./immudb service start      -  Starts initialized daemon
sudo ./immudb service restart    -  Restarts daemon
sudo ./immudb service uninstall  -  Removes daemon and its setup
Uninstall immudb after 20 second
sudo ./immudb service install --time 20 immudb`)

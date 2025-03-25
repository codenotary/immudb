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

package service

var ConfigImmuClient = []byte(`dir = "%programdata%\\Immuclient"
address = "0.0.0.0"
port = 3323
immudb-address = "127.0.0.1"
immudb-port = 3322
pidfile = "%programdata%\\Immuclient\\config\\immuclient.pid"
logfile = "%programdata%\\Immuclient\\config\\immuclient.log"
mtls = false
detached = false
servername = "localhost"
audit-username = "immudb"
audit-password = "immudb"
pkey = ""
certificate = ""
clientcas = ""`)

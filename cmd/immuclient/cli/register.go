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

package cli

type command struct {
	name     string
	short    string
	command  func(args []string) (string, error)
	args     []string
	variable bool
}

func (cli *cli) initCommands() {
	// Auth
	cli.Register(&command{"login", "Login using the specified username and password", cli.login, []string{"username"}, false})
	cli.Register(&command{"logout", "", cli.logout, nil, false})
	cli.Register(&command{"use", "Select database", cli.UseDatabase, []string{"databasename"}, false})

	// Get commands
	cli.Register(&command{"safeget", "Get and verify item having the specified key", cli.safeGetKey, []string{"key"}, false})
	cli.Register(&command{"get", "Get item having the specified key", cli.getKey, []string{"key"}, false})
	cli.Register(&command{"gettx", "Return a tx by id", cli.getTxByID, []string{"id"}, false})

	// Set commands
	cli.Register(&command{"set", "Add new item having the specified key and value", cli.set, []string{"key", "value"}, false})
	cli.Register(&command{"safeset", "Add and verify new item having the specified key and value", cli.safeset, []string{"key", "value"}, false})
	cli.Register(&command{"restore", "Restore older value of the key", cli.restore, []string{"key"}, false})
	cli.Register(&command{"safezadd", "Add and verify new key with score to a new or existing sorted set", cli.safeZAdd, []string{"setname", "score", "key"}, false})
	cli.Register(&command{"zadd", "Add new key with score to a new or existing sorted set", cli.zAdd, []string{"setname", "score", "key"}, false})

	cli.Register(&command{"delete", "Delete item having the specified key", cli.deleteKey, []string{"key"}, false})

	// Current status commands
	cli.Register(&command{"health", "Return the number of pending requests and the time the last request was completed", cli.health, nil, false})
	cli.Register(&command{"current", "Return the last tx and hash stored locally", cli.currentState, nil, false})

	// Reference commands
	cli.Register(&command{"reference", "Add new reference to an existing key", cli.reference, []string{"refkey", "key"}, false})
	cli.Register(&command{"safereference", "Add and verify new reference to an existing key", cli.safereference, []string{"refkey", "key"}, false})

	// Scannner commands
	cli.Register(&command{"scan", "Iterate over keys having the specified prefix", cli.scan, []string{"prefix"}, false})
	cli.Register(&command{"zscan", "Iterate over a sorted set", cli.zScan, []string{"prefix"}, false})
	cli.Register(&command{"count", "Count keys having the specified prefix", cli.count, []string{"prefix"}, false})

	// Misc commands
	cli.Register(&command{"status", "", cli.healthCheck, nil, false})
	cli.Register(&command{"history", "Fetch history for the item having the specified key", cli.history, []string{"key"}, false})
	cli.Register(&command{"version", "Print version", cli.version, nil, false})
	cli.Register(&command{"info", "Print server information", cli.serverInfo, nil, false})

	// SQL
	cli.Register(&command{"exec", "Executes sql statement", cli.sqlExec, []string{"statement"}, true})
	cli.Register(&command{"query", "Query sql statement", cli.sqlQuery, []string{"statement"}, true})
	cli.Register(&command{"describe", "Describe table", cli.describeTable, []string{"table"}, false})
	cli.Register(&command{"tables", "List tables", cli.listTables, nil, false})
}

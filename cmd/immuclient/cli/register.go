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
	cli.Register(&command{"rawsafeget", "Get item having the specified key, without parsing structured values", cli.rawSafeGetKey, []string{"key"}, false})
	cli.Register(&command{"safeget", "Get and verify item having the specified key", cli.safeGetKey, []string{"key"}, false})
	cli.Register(&command{"get", "Get item having the specified key", cli.getKey, []string{"key"}, false})
	cli.Register(&command{"getbyindex", "Return an element by index", cli.getByIndex, []string{"index"}, false})
	cli.Register(&command{"getrawbysafeindex", "Return an element by index ", cli.getRawBySafeIndex, []string{"index"}, false})

	// Set commands
	cli.Register(&command{"set", "Add new item having the specified key and value", cli.set, []string{"key", "value"}, false})
	cli.Register(&command{"safeset", "Add and verify new item having the specified key and value", cli.safeset, []string{"key", "value"}, false})
	cli.Register(&command{"rawsafeset", "Set item having the specified key, without setup structured values", cli.rawSafeSet, []string{"key", "value"}, false})
	cli.Register(&command{"safezadd", "Add and verify new key with score to a new or existing sorted set", cli.safeZAdd, []string{"setname", "score", "key"}, false})
	cli.Register(&command{"zadd", "Add new key with score to a new or existing sorted set", cli.zAdd, []string{"setname", "score", "key"}, false})

	// Tamperproofing commands
	cli.Register(&command{"check-consistency", "Check consistency for the specified index and hash", cli.consistency, []string{"index", "hash"}, false})
	cli.Register(&command{"inclusion", "Check if specified index is included in the current tree", cli.inclusion, []string{"index"}, false})

	// Current status commands
	cli.Register(&command{"current", "Return the last merkle tree root and index stored locally", cli.currentRoot, nil, false})

	// Reference commands
	cli.Register(&command{"reference", "Add new reference to an existing key", cli.reference, []string{"refkey", "key"}, false})
	cli.Register(&command{"safereference", "Add and verify new reference to an existing key", cli.safereference, []string{"refkey", "key"}, false})

	// Scannner commands
	cli.Register(&command{"scan", "Iterate over keys having the specified prefix", cli.scan, []string{"prefix"}, false})
	cli.Register(&command{"zscan", "Iterate over a sorted set", cli.zScan, []string{"prefix"}, false})
	cli.Register(&command{"iscan", "Iterate over all elements by insertion order", cli.iScan, []string{"pagenumber", "pagesize"}, false})
	cli.Register(&command{"count", "Count keys having the specified prefix", cli.count, []string{"prefix"}, false})

	// Misc commands
	cli.Register(&command{"status", "", cli.healthCheck, nil, false})
	cli.Register(&command{"history", "Fetch history for the item having the specified key", cli.history, []string{"key"}, false})
	cli.Register(&command{"version", "Print version", cli.version, nil, false})
}

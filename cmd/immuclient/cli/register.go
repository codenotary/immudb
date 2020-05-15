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
	name    string
	short   string
	command func(args []string) (string, error)
	args    []string
}

func (cli *cli) initCommands() {
	// Auth
	cli.Register(&command{"login", "Login using the specified username and password", cli.login, []string{"username"}})
	cli.Register(&command{"logout", "", cli.logout, nil})

	// Get commands
	cli.Register(&command{"rawsafeget", "Get item having the specified key, without parsing structured values", cli.rawSafeGetKey, []string{"key"}})
	cli.Register(&command{"safeget", "Get and verify item having the specified key", cli.safeGetKey, []string{"key"}})
	cli.Register(&command{"get", "Get item having the specified key", cli.getKey, []string{"key"}})
	cli.Register(&command{"getbyindex", "Return an element by index", cli.getByIndex, []string{"index"}})

	// Set commands
	cli.Register(&command{"set", "Add new item having the specified key and value", cli.set, []string{"key", "value"}})
	cli.Register(&command{"safeset", "Add and verify new item having the specified key and value", cli.safeset, []string{"key", "value"}})
	cli.Register(&command{"rawsafeset", "Set item having the specified key, without setup structured values", cli.rawSafeSet, []string{"key", "value"}})
	cli.Register(&command{"safezadd", "Add and verify new key with score to a new or existing sorted set", cli.safeZAdd, []string{"setname", "score", "key"}})
	cli.Register(&command{"zadd", "Add new key with score to a new or existing sorted set", cli.zAdd, []string{"setname", "score", "key"}})

	// Tamperproofing commands
	cli.Register(&command{"check-consistency", "Check consistency for the specified index and hash", cli.consistency, []string{"index", "hash"}})
	cli.Register(&command{"inclusion", "Check if specified index is included in the current tree", cli.inclusion, []string{"index"}})

	// Current status commands
	cli.Register(&command{"current", "Return the last merkle tree root and index stored locally", cli.currentRoot, nil})

	// Reference commands
	cli.Register(&command{"reference", "Add new reference to an existing key", cli.reference, []string{"refkey", "key"}})
	cli.Register(&command{"safereference", "Add and verify new reference to an existing key", cli.safereference, []string{"refkey", "key"}})

	// Scannner commands
	cli.Register(&command{"scan", "Iterate over keys having the specified prefix", cli.scan, []string{"prefix"}})
	cli.Register(&command{"zscan", "Iterate over a sorted set", cli.zScan, []string{"prefix"}})
	cli.Register(&command{"iscan", "Iterate over all elements by insertion order", cli.iScan, []string{"pagenumber", "pagesize"}})
	cli.Register(&command{"count", "Count keys having the specified prefix", cli.count, []string{"prefix"}})

	// Misc commands
	cli.Register(&command{"status", "", cli.healthCheck, nil})
	cli.Register(&command{"history", "Fetch history for the item having the specified key", cli.history, []string{"key"}})
}

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

package main

import (
	c "github.com/codenotary/immudb/cmd/helper"
	immugw "github.com/codenotary/immudb/cmd/immugw/command"
	"github.com/codenotary/immudb/cmd/version"
	"os"
)

func main() {
	version.App = "immugw"
	cmd := immugw.NewCmd()
	if err := cmd.Execute(); err != nil {
		c.QuitWithUserError(err)
		os.Exit(1)
	}
	os.Exit(0)
}

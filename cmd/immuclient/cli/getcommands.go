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

package cli

import "github.com/codenotary/immudb/cmd/immuclient/immuc"

func (cli *cli) getTxByID(args []string) (immuc.CommandOutput, error) {
	return cli.immucl.GetTxByID(args)
}

func (cli *cli) getKey(args []string) (immuc.CommandOutput, error) {
	return cli.immucl.Get(args)
}

func (cli *cli) safeGetKey(args []string) (immuc.CommandOutput, error) {
	return cli.immucl.VerifiedGet(args)
}

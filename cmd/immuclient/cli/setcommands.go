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

func (cli *cli) rawSafeSet(args []string) (string, error) {
	return cli.immucl.RawSafeSet(args)
}

func (cli *cli) set(args []string) (string, error) {
	return cli.immucl.Set(args)
}

func (cli *cli) safeset(args []string) (string, error) {
	return cli.immucl.SafeSet(args)
}

func (cli *cli) zAdd(args []string) (string, error) {
	return cli.immucl.ZAdd(args)
}

func (cli *cli) safeZAdd(args []string) (string, error) {
	return cli.immucl.SafeZAdd(args)
}

func (cli *cli) UseDatabase(args []string) (string, error) {
	return cli.immucl.UseDatabase(args)
}

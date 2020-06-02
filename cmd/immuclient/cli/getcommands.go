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

func (cli *cli) getByIndex(args []string) (string, error) {
	return cli.immucl.GetByIndex(args)
}

func (cli *cli) getKey(args []string) (string, error) {
	return cli.immucl.GetKey(args)
}

func (cli *cli) rawSafeGetKey(args []string) (string, error) {
	return cli.immucl.RawSafeGetKey(args)
}

func (cli *cli) safeGetKey(args []string) (string, error) {
	return cli.immucl.SafeGetKey(args)
}

func (cli *cli) getRawBySafeIndex(args []string) (string, error) {
	return cli.immucl.GetRawBySafeIndex(args)
}

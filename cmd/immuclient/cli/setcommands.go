/*
Copyright 2024 Codenotary Inc. All rights reserved.

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

func (cli *cli) set(args []string) (string, error) {
	return cli.immucl.Set(args)
}

func (cli *cli) safeset(args []string) (string, error) {
	return cli.immucl.VerifiedSet(args)
}

func (cli *cli) restore(args []string) (string, error) {
	return cli.immucl.Restore(args)
}

func (cli *cli) deleteKey(args []string) (string, error) {
	return cli.immucl.DeleteKey(args)
}

func (cli *cli) zAdd(args []string) (string, error) {
	return cli.immucl.ZAdd(args)
}

func (cli *cli) safeZAdd(args []string) (string, error) {
	return cli.immucl.VerifiedZAdd(args)
}

func (cli *cli) UseDatabase(args []string) (string, error) {
	return cli.immucl.UseDatabase(args)
}

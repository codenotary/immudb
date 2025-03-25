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

func (cli *cli) sqlExec(args []string) (string, error) {
	return cli.immucl.SQLExec(args)
}

func (cli *cli) sqlQuery(args []string) (string, error) {
	return cli.immucl.SQLQuery(args)
}

func (cli *cli) describeTable(args []string) (string, error) {
	return cli.immucl.DescribeTable(args)
}

func (cli *cli) listTables(args []string) (string, error) {
	return cli.immucl.ListTables()
}

func (cli *cli) useDatabase(args []string) (string, error) {
	return cli.immucl.UseDatabase(args)
}

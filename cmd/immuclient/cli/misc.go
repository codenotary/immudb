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

import (
	"github.com/codenotary/immudb/cmd/version"
)

func (cli *cli) history(args []string) (string, error) {
	return cli.immucl.History(args)
}

func (cli *cli) healthCheck(args []string) (string, error) {
	return cli.immucl.HealthCheck(args)
}

func (cli *cli) version(args []string) (string, error) {
	return version.VersionStr(), nil
}

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

package man

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
	"github.com/spf13/cobra/doc"
)

// Generate return command that generates man files for a specified command
func Generate(cmd *cobra.Command, title string, defaultDir string) *cobra.Command {
	cmd.DisableAutoGenTag = false
	return &cobra.Command{
		Use:    "mangen [dir]",
		Short:  "Generate man files in the specified directory",
		Hidden: true,
		Args:   cobra.MinimumNArgs(0),
		RunE: func(mangenCmd *cobra.Command, args []string) (err error) {
			header := &doc.GenManHeader{
				Title:   title,
				Section: "1",
			}
			dir := defaultDir
			if len(args) > 0 {
				dir = args[0]
			}
			_ = os.Mkdir(dir, os.ModePerm)
			if err := doc.GenManTree(cmd, header, dir); err == nil {
				fmt.Printf("SUCCESS: man files generated in the %s directory\n", dir)
			}
			return err
		},
		DisableAutoGenTag: false,
	}
}

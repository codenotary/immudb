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

package sservice

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/require"
)

func TestManpageService(t *testing.T) {

	manDir := filepath.Join(t.TempDir(), "man_dir_test")

	t.Run("install", func(t *testing.T) {
		rootCmd := &cobra.Command{Use: "test"}
		versionCmd := &cobra.Command{Use: "version", Run: func(cmd *cobra.Command, args []string) {}}
		rootCmd.AddCommand(versionCmd)

		mps := manpageService{}

		require.NoError(t, mps.InstallManPages(manDir, "test", rootCmd))
		manFiles, err := ioutil.ReadDir(manDir)
		require.NoError(t, err)
		require.Equal(t, 2, len(manFiles))
	})

	t.Run("uninstall", func(t *testing.T) {
		mps := manpageService{}

		_, err := ioutil.ReadDir(manDir)
		require.NoError(t, err)

		require.NoError(t, mps.UninstallManPages(manDir, "test"))
		manFiles, err := ioutil.ReadDir(manDir)
		require.NoError(t, err)
		require.Empty(t, manFiles)
		os.RemoveAll(manDir)
	})
}

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

package man

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/spf13/cobra"
)

func TestGenerate(t *testing.T) {
	rootCmd := &cobra.Command{
		Use:   "somecommand somearg1",
		Short: "somme command short description",
		Long:  "some command long description",
	}
	dir := "./test_generate_output"
	defer os.RemoveAll(dir)
	cmd := Generate(rootCmd, rootCmd.Use, dir)
	cmd.SetArgs([]string{dir})
	require.NoError(t, cmd.Execute())
	bs, err := ioutil.ReadFile(filepath.Join(dir, "somecommand.1"))
	require.NoError(t, err)
	require.NotEmpty(t, bs)
}

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

package sservice

import (
	"io/ioutil"
	"os"
	"testing"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/require"
)

func TestManpageService_InstallManPages(t *testing.T) {
	rootCmd := &cobra.Command{Use: "test"}
	versionCmd := &cobra.Command{Use: "version", Run: func(cmd *cobra.Command, args []string) {}}
	rootCmd.AddCommand(versionCmd)

	mps := manpageService{}

	manDir := "./man_dir_test"

	require.NoError(t, mps.InstallManPages(manDir, "test", rootCmd))
	manFiles, err := ioutil.ReadDir(manDir)
	require.NoError(t, err)
	require.Equal(t, 2, len(manFiles))
}

func TestManpageService_UninstallManPages(t *testing.T) {
	mps := manpageService{}

	manDir := "./man_dir_test"
	_, err := ioutil.ReadDir(manDir)

	require.NoError(t, mps.UninstallManPages(manDir, "test"))
	manFiles, err := ioutil.ReadDir(manDir)
	require.NoError(t, err)
	require.Empty(t, manFiles)
	os.RemoveAll(manDir)
}

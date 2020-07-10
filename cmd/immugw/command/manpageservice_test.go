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

package immugw

import (
	"github.com/stretchr/testify/require"
	"io/ioutil"
	"os"
	"testing"
)

func TestManpageServiceImmugw_InstallManPages(t *testing.T) {
	mps := ManpageServiceImmugw{}

	manDir := "./man_dir_immugw_test"

	require.NoError(t, mps.InstallManPages(manDir))
	manFiles, err := ioutil.ReadDir(manDir)
	require.NoError(t, err)
	require.Equal(t, 2, len(manFiles))
}

func TestManpageServiceImmugw_UninstallManPages(t *testing.T) {
	mps := ManpageServiceImmugw{}

	manDir := "./man_dir_immugw_test"
	manFiles, err := ioutil.ReadDir(manDir)

	require.NoError(t, mps.UninstallManPages(manDir))
	manFiles, err = ioutil.ReadDir(manDir)
	require.NoError(t, err)
	require.Empty(t, manFiles)
	os.RemoveAll(manDir)
}

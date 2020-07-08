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
	"io/ioutil"
	"os"
	"testing"

	"github.com/codenotary/immudb/cmd/version"
	"github.com/codenotary/immudb/pkg/gw"
	"github.com/stretchr/testify/require"
)

func TestNewCmd(t *testing.T) {
	version.App = "immugw"
	// viper.
	cmd := NewCmd(new(gw.ImmuGwServerMock))
	require.NoError(t, cmd.Execute())

	manDir := "./man_dir_immugw_test"

	require.NoError(t, InstallManPages(manDir))
	manFiles, err := ioutil.ReadDir(manDir)
	require.NoError(t, err)
	require.Equal(t, 2, len(manFiles))

	require.NoError(t, UnistallManPages(manDir))
	manFiles, err = ioutil.ReadDir(manDir)
	require.NoError(t, err)
	require.Empty(t, manFiles)
	os.RemoveAll(manDir)
}

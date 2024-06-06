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

package helper

import (
	"io/ioutil"
	"os"
	"os/user"
	"testing"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestOptions_InitConfig(t *testing.T) {
	input, _ := ioutil.ReadFile("../../test/immudb.toml")
	user, err := user.Current()
	require.NoError(t, err)

	fn := user.HomeDir + "/immudbtest9990.toml"
	_ = ioutil.WriteFile(fn, input, 0644)
	defer os.RemoveAll(fn)
	o := Config{}
	o.Init("test")
	address := viper.GetString("address")
	assert.NotNil(t, address)
}

func TestOptions_InitConfigWithCfFn(t *testing.T) {
	input, _ := ioutil.ReadFile("../../test/immudb.toml")
	fn := "/tmp/immudbtest9991.toml"
	_ = ioutil.WriteFile(fn, input, 0644)
	defer os.RemoveAll(fn)
	o := Config{
		CfgFn: fn,
	}
	o.Init("test")
	address := viper.GetString("address")
	assert.NotNil(t, address)
}

func TestConfig_Load(t *testing.T) {
	input, _ := ioutil.ReadFile("../../test/immudb.toml")
	fn := "/tmp/immudbtest9991.toml"
	_ = ioutil.WriteFile(fn, input, 0644)
	defer os.RemoveAll(fn)
	o := Config{
		CfgFn: fn,
	}
	o.Init("test")
	address := viper.GetString("address")
	assert.NotNil(t, address)
	cmd := cobra.Command{}
	cmd.Flags().StringVar(&o.CfgFn, "config", "", "config file")
	err := o.LoadConfig(&cmd)
	assert.NoError(t, err)
}

func TestConfig_LoadError(t *testing.T) {
	input, _ := ioutil.ReadFile("../../test/immudb.toml")
	fn := "/tmp/immudbtest9991.toml"
	_ = ioutil.WriteFile(fn, input, 0644)
	defer os.RemoveAll(fn)
	o := Config{
		CfgFn: fn,
	}
	o.Init("test")
	address := viper.GetString("address")
	assert.NotNil(t, address)
	cmd := cobra.Command{}
	err := o.LoadConfig(&cmd)
	assert.Error(t, err)
}

func TestConfig_LoadError2(t *testing.T) {
	fn := "/tmp/immudbtest9991.toml"
	o := Config{
		CfgFn: fn,
	}
	o.Init("test")
	address := viper.GetString("address")
	assert.NotNil(t, address)
	cmd := cobra.Command{}
	cmd.Flags().StringVar(&o.CfgFn, "config", "", "config file")
	err := o.LoadConfig(&cmd)
	assert.Error(t, err)
}

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

package helper

import (
	"io/ioutil"
	"log"
	"os"
	"testing"

	"github.com/mitchellh/go-homedir"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
)

func TestOptions_InitConfig(t *testing.T) {
	input, _ := ioutil.ReadFile("../../test/immudb.toml")
	home, err := homedir.Dir()
	if err != nil {
		log.Fatal(err)
	}
	fn := home + "/immudbtest9990.toml"
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
	assert.Nil(t, err)
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

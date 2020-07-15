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
	"github.com/mitchellh/go-homedir"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
	"io/ioutil"
	"log"
	"os"
	"testing"
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
	o := Options{}
	o.InitConfig("test")
	address := viper.GetString("address")
	assert.NotNil(t, address)
}
func TestOptions_InitConfigWithCfFn(t *testing.T) {
	input, _ := ioutil.ReadFile("../../test/immudb.toml")
	fn := "/tmp/immudbtest9991.toml"
	_ = ioutil.WriteFile(fn, input, 0644)
	defer os.RemoveAll(fn)
	o := Options{
		CfgFn: fn,
	}
	o.InitConfig("test")
	address := viper.GetString("address")
	assert.NotNil(t, address)
}

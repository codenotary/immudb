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
	"fmt"
	"github.com/mitchellh/go-homedir"
	"github.com/spf13/viper"
	"os"
	"runtime"
	"strings"
)

// Options cmd options
type Options struct {
	CfgFn string
}

// InitConfig initializes config
func (o *Options) InitConfig(name string) {
	if o.CfgFn != "" {
		viper.SetConfigFile(o.CfgFn)
	} else {
		home, err := homedir.Dir()
		if err != nil {
			QuitToStdErr(err)
		}
		viper.AddConfigPath("configs")
		viper.AddConfigPath(os.Getenv("GOPATH") + "/src/configs")
		if runtime.GOOS != "windows" {
			viper.AddConfigPath("/etc/immudb")
		}
		viper.AddConfigPath(home)
		viper.SetConfigName(name)
	}
	viper.SetEnvPrefix(strings.ToUpper(name))
	viper.SetEnvKeyReplacer(strings.NewReplacer("-", "_"))
	viper.AutomaticEnv()
	if err := viper.ReadInConfig(); err == nil {
		o.CfgFn = viper.ConfigFileUsed()
		fmt.Println("Using config file:", o.CfgFn)
	}
}

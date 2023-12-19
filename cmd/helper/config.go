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
	"os"
	"os/user"
	"strings"

	service "github.com/codenotary/immudb/cmd/immuclient/service/constants"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

// Options cmd options
type Config struct {
	Name  string // default config file name
	CfgFn string // bind with flag config (config file submitted by user, it overrides default)
}

// Init initializes config
func (c *Config) Init(name string) error {
	if c.CfgFn != "" {
		viper.SetConfigFile(c.CfgFn)
	} else {
		if user, err := user.Current(); err != nil {
			return err
		} else {
			viper.AddConfigPath(user.HomeDir)
		}
		viper.AddConfigPath("../src/configs")
		viper.AddConfigPath(os.Getenv("GOPATH") + "/src/configs")
		if path, _ := os.Executable(); path == service.ExecPath {
			viper.AddConfigPath("/etc/" + name)
		}
		viper.SetConfigName(name)
	}
	viper.SetEnvPrefix(strings.ToUpper(name))
	viper.SetEnvKeyReplacer(strings.NewReplacer("-", "_"))
	viper.AutomaticEnv()
	if err := viper.ReadInConfig(); err == nil {
		c.CfgFn = viper.ConfigFileUsed()
	} else {
		return err
	}
	return nil
}

// LoadConfig loads the config file (if any) and initializes the config
func (c *Config) LoadConfig(cmd *cobra.Command) (err error) {
	if c.CfgFn, err = cmd.Flags().GetString("config"); err != nil {
		return err
	}
	if err = c.Init(c.Name); err != nil {
		if !strings.Contains(err.Error(), "Not Found") {
			return err
		}
	}
	return nil
}

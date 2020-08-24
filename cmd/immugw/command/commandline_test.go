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
	"testing"

	"github.com/codenotary/immudb/cmd/helper"
	"github.com/stretchr/testify/assert"

	"github.com/spf13/cobra"
)

func TestCommandline_ConfigChain(t *testing.T) {
	cmd := &cobra.Command{}
	c := Commandline{
		config: helper.Config{Name: "test"},
	}
	f := func(cmd *cobra.Command, args []string) error {
		return nil
	}
	cmd.Flags().StringVar(&c.config.CfgFn, "config", "", "config file")
	cc := c.ConfigChain(f)
	err := cc(cmd, []string{})
	assert.Nil(t, err)
}

func TestCommandline_ConfigChainErr(t *testing.T) {
	cmd := &cobra.Command{}

	c := Commandline{}
	f := func(cmd *cobra.Command, args []string) error {
		return nil
	}

	cc := c.ConfigChain(f)

	err := cc(cmd, []string{})
	assert.Error(t, err)
}

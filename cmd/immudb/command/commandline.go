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

package immudb

import (
	c "github.com/codenotary/immudb/cmd/helper"
	"github.com/spf13/cobra"
)

// Commandline ...
type Commandline struct {
	config c.Config
	P      c.Plauncher
}

func (cl *Commandline) ConfigChain(post func(cmd *cobra.Command, args []string) error) func(cmd *cobra.Command, args []string) (err error) {
	return func(cmd *cobra.Command, args []string) (err error) {
		if err = cl.config.LoadConfig(cmd); err != nil {
			return err
		}
		if post != nil {
			return post(cmd, args)
		}
		return nil
	}
}

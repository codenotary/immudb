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

package immuclient

import (
	"bufio"
	"bytes"
	"context"
	"io"
	"io/ioutil"
	"os"

	c "github.com/codenotary/immudb/cmd/helper"
	"github.com/spf13/cobra"
)

func (cl *commandline) reference(cmd *cobra.Command) {
	ccmd := &cobra.Command{
		Use:               "reference refkey key",
		Short:             "Add new reference to an existing key",
		Aliases:           []string{"r"},
		PersistentPreRunE: cl.connect,
		PersistentPostRun: cl.disconnect,
		RunE: func(cmd *cobra.Command, args []string) error {
			var reader io.Reader
			if len(args) > 1 {
				reader = bytes.NewReader([]byte(args[1]))
			} else {
				reader = bufio.NewReader(os.Stdin)
			}
			reference, err := ioutil.ReadAll(bytes.NewReader([]byte(args[0])))
			if err != nil {
				c.QuitToStdErr(err)
			}
			var buf bytes.Buffer
			tee := io.TeeReader(reader, &buf)
			key, err := ioutil.ReadAll(tee)
			if err != nil {
				c.QuitToStdErr(err)
			}
			ctx := context.Background()
			response, err := cl.ImmuClient.Reference(ctx, reference, key)
			if err != nil {
				c.QuitWithUserError(err)
			}
			value, err := ioutil.ReadAll(&buf)
			if err != nil {
				c.QuitToStdErr(err)
			}
			printItem([]byte(args[0]), value, response)
			return nil
		},
		Args: cobra.MinimumNArgs(1),
	}
	cmd.AddCommand(ccmd)
}

func (cl *commandline) safereference(cmd *cobra.Command) {
	ccmd := &cobra.Command{
		Use:               "safereference refkey key",
		Short:             "Add and verify new reference to an existing key",
		Aliases:           []string{"sr"},
		PersistentPreRunE: cl.connect,
		PersistentPostRun: cl.disconnect,
		RunE: func(cmd *cobra.Command, args []string) error {
			var reader io.Reader
			if len(args) > 1 {
				reader = bytes.NewReader([]byte(args[1]))
			} else {
				reader = bufio.NewReader(os.Stdin)
			}
			reference, err := ioutil.ReadAll(bytes.NewReader([]byte(args[0])))
			if err != nil {
				c.QuitToStdErr(err)
			}
			var buf bytes.Buffer
			tee := io.TeeReader(reader, &buf)
			key, err := ioutil.ReadAll(tee)
			if err != nil {
				c.QuitToStdErr(err)
			}
			ctx := context.Background()
			response, err := cl.ImmuClient.SafeReference(ctx, reference, key)
			if err != nil {
				c.QuitWithUserError(err)
			}
			value, err := ioutil.ReadAll(&buf)
			if err != nil {
				c.QuitToStdErr(err)
			}
			printItem([]byte(args[0]), value, response)
			return nil
		},
		Args: cobra.MinimumNArgs(1),
	}
	cmd.AddCommand(ccmd)
}

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
	"context"
	"encoding/hex"
	"fmt"
	"strconv"

	c "github.com/codenotary/immudb/cmd/helper"
	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/spf13/cobra"
)

func (cl *commandline) consistency(cmd *cobra.Command) {
	ccmd := &cobra.Command{
		Use:               "check-consistency index hash",
		Short:             "Check consistency for the specified index and hash",
		Aliases:           []string{"c"},
		PersistentPreRunE: cl.connect,
		PersistentPostRun: cl.disconnect,
		RunE: func(cmd *cobra.Command, args []string) error {
			index, err := strconv.ParseUint(args[0], 10, 64)
			if err != nil {
				c.QuitToStdErr(err)
			}
			ctx := context.Background()
			proof, err := cl.ImmuClient.Consistency(ctx, index)
			if err != nil {
				c.QuitWithUserError(err)
			}

			var root []byte
			src := []byte(args[1])
			l := hex.DecodedLen(len(src))
			if l != 32 {
				c.QuitToStdErr(fmt.Errorf("invalid hash length"))
			}
			root = make([]byte, l)
			_, err = hex.Decode(root, src)
			if err != nil {
				c.QuitToStdErr(err)
			}

			fmt.Printf("verified: %t \nfirstRoot: %x at index: %d \nsecondRoot: %x at index: %d \n",
				proof.Verify(schema.Root{Index: index, Root: root}),
				proof.FirstRoot,
				proof.First,
				proof.SecondRoot,
				proof.Second)
			return nil
		},
		Args: cobra.MinimumNArgs(2),
	}
	cmd.AddCommand(ccmd)
}

func (cl *commandline) inclusion(cmd *cobra.Command) {
	ccmd := &cobra.Command{
		Use:               "inclusion index",
		Short:             "Check if specified index is included in the current tree",
		Aliases:           []string{"i"},
		PersistentPreRunE: cl.connect,
		PersistentPostRun: cl.disconnect,
		RunE: func(cmd *cobra.Command, args []string) error {
			index, err := strconv.ParseUint(args[0], 10, 64)
			if err != nil {
				c.QuitToStdErr(err)
			}
			ctx := context.Background()
			proof, err := cl.ImmuClient.Inclusion(ctx, index)
			if err != nil {
				c.QuitWithUserError(err)
			}
			var hash []byte
			if len(args) > 1 {
				src := []byte(args[1])
				l := hex.DecodedLen(len(src))
				if l != 32 {
					c.QuitToStdErr(fmt.Errorf("invalid hash length"))
				}
				hash = make([]byte, l)
				_, err = hex.Decode(hash, src)
				if err != nil {
					c.QuitToStdErr(err)
				}
			} else {
				item, err := cl.ImmuClient.ByIndex(ctx, index)
				if err != nil {
					c.QuitWithUserError(err)
				}
				hash, err = item.Hash()
				if err != nil {
					c.QuitWithUserError(err)
				}
			}
			fmt.Printf("verified: %t \nhash: %x at index: %d \nroot: %x at index: %d \n",
				proof.Verify(index, hash),
				proof.Leaf,
				proof.Index,
				proof.Root,
				proof.At)
			return nil
		},
		Args: cobra.MinimumNArgs(1),
	}
	cmd.AddCommand(ccmd)
}

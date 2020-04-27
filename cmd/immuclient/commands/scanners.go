package commands

import (
	"bytes"
	"context"
	"encoding/hex"
	"fmt"
	"io/ioutil"
	"strconv"

	c "github.com/codenotary/immudb/cmd"
	"github.com/spf13/cobra"
)

func (cl *commandline) zScanSetName(cmd *cobra.Command) {
	ccmd := &cobra.Command{
		Use:     "zscan setname",
		Short:   "Iterate over a sorted set",
		Aliases: []string{"zscn"},
		RunE: func(cmd *cobra.Command, args []string) error {
			set, err := ioutil.ReadAll(bytes.NewReader([]byte(args[0])))
			if err != nil {
				c.QuitToStdErr(err)
			}
			ctx := context.Background()
			response, err := cl.immuClient.ZScan(ctx, set)
			if err != nil {
				c.QuitWithUserError(err)
			}
			for _, item := range response.Items {
				printItem(nil, nil, item)
				fmt.Println()
			}
			return nil
		},
		Args: cobra.ExactArgs(1),
	}
	cmd.AddCommand(ccmd)
}

func (cl *commandline) iScanPageNumPageSize(cmd *cobra.Command) {
	ccmd := &cobra.Command{
		Use:     "iscan pagenumber pagesize",
		Short:   "Iterate over all elements by insertion order",
		Aliases: []string{"iscn"},
		RunE: func(cmd *cobra.Command, args []string) error {
			pageNumber, err := strconv.ParseUint(args[0], 10, 64)
			if err != nil {
				c.QuitToStdErr(err)
			}
			pageSize, err := strconv.ParseUint(args[1], 10, 64)
			if err != nil {
				c.QuitToStdErr(err)
			}
			ctx := context.Background()
			response, err := cl.immuClient.IScan(ctx, pageNumber, pageSize)
			if err != nil {
				c.QuitWithUserError(err)
			}
			for _, item := range response.Items {
				printItem(nil, nil, item)
				fmt.Println()
			}
			return nil
		},
		Args: cobra.ExactArgs(2),
	}
	cmd.AddCommand(ccmd)
}

func (cl *commandline) iScanPrefix(cmd *cobra.Command) {
	ccmd := &cobra.Command{
		Use:     "scan prefix",
		Short:   "Iterate over keys having the specified prefix",
		Aliases: []string{"scn"},
		RunE: func(cmd *cobra.Command, args []string) error {

			prefix, err := ioutil.ReadAll(bytes.NewReader([]byte(args[0])))
			if err != nil {
				c.QuitToStdErr(err)
			}
			ctx := context.Background()
			response, err := cl.immuClient.Scan(ctx, prefix)
			if err != nil {
				c.QuitWithUserError(err)
			}
			for _, item := range response.Items {
				printItem(nil, nil, item)
				fmt.Println()
			}
			return nil
		},
		Args: cobra.ExactArgs(1),
	}
	cmd.AddCommand(ccmd)
}

func (cl *commandline) countPrefix(cmd *cobra.Command) {
	ccmd := &cobra.Command{
		Use:     "count prefix",
		Short:   "Count keys having the specified prefix",
		Aliases: []string{"cnt"},
		RunE: func(cmd *cobra.Command, args []string) error {

			prefix, err := ioutil.ReadAll(bytes.NewReader([]byte(args[0])))
			if err != nil {
				c.QuitToStdErr(err)
			}
			ctx := context.Background()
			response, err := cl.immuClient.Count(ctx, prefix)
			if err != nil {
				c.QuitWithUserError(err)
			}
			fmt.Println(response.Count)
			return nil
		},
		Args: cobra.ExactArgs(1),
	}
	cmd.AddCommand(ccmd)
}

func (cl *commandline) inclusionIndex(cmd *cobra.Command) {
	ccmd := &cobra.Command{
		Use:     "inclusion index",
		Short:   "Check if specified index is included in the current tree",
		Aliases: []string{"i"},
		RunE: func(cmd *cobra.Command, args []string) error {
			index, err := strconv.ParseUint(args[0], 10, 64)
			if err != nil {
				c.QuitToStdErr(err)
			}
			ctx := context.Background()
			proof, err := cl.immuClient.Inclusion(ctx, index)
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
				item, err := cl.immuClient.ByIndex(ctx, index)
				if err != nil {
					c.QuitWithUserError(err)
				}
				hash, err = item.Hash()

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

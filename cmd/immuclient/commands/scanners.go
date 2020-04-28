package commands

import (
	"bytes"
	"context"
	"fmt"
	"io/ioutil"
	"strconv"

	c "github.com/codenotary/immudb/cmd"
	"github.com/spf13/cobra"
)

func (cl *commandline) zScan(cmd *cobra.Command) {
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

func (cl *commandline) iScan(cmd *cobra.Command) {
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

func (cl *commandline) scan(cmd *cobra.Command) {
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

func (cl *commandline) count(cmd *cobra.Command) {
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

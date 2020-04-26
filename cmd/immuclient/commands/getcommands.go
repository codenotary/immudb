package commands

import (
	"bytes"
	"context"
	"io/ioutil"
	"strconv"

	c "github.com/codenotary/immudb/cmd"
	"github.com/spf13/cobra"
)

func (cl *commandline) getByIndex(cmd *cobra.Command) {
	ccmd := &cobra.Command{
		Use:     "getByIndex",
		Short:   "Return an element by index",
		Aliases: []string{"bi"},
		RunE: func(cmd *cobra.Command, args []string) error {
			index, err := strconv.ParseUint(args[0], 10, 64)
			if err != nil {
				c.QuitToStdErr(err)
			}
			ctx := context.Background()
			response, err := cl.immuClient.ByIndex(ctx, index)
			if err != nil {
				c.QuitWithUserError(err)
			}
			printByIndex(response)
			return nil
		},
		Args: cobra.ExactArgs(1),
	}
	cmd.AddCommand(ccmd)
}

func (cl *commandline) getKey(cmd *cobra.Command) {
	ccmd := &cobra.Command{
		Use:     "get key",
		Short:   "Get item having the specified key",
		Aliases: []string{"g"},
		RunE: func(cmd *cobra.Command, args []string) error {

			key, err := ioutil.ReadAll(bytes.NewReader([]byte(args[0])))
			if err != nil {
				c.QuitToStdErr(err)
			}
			ctx := context.Background()
			response, err := cl.immuClient.Get(ctx, key)
			if err != nil {
				c.QuitWithUserError(err)
			}
			printItem([]byte(args[0]), nil, response)
			return nil
		},
		Args: cobra.ExactArgs(1),
	}
	cmd.AddCommand(ccmd)
}

func (cl *commandline) rawSafeGetKey(cmd *cobra.Command) {
	ccmd := &cobra.Command{
		Use:     "rawsafeget key",
		Short:   "Get item having the specified key, without parsing structured values",
		Aliases: []string{"rg"},
		RunE: func(cmd *cobra.Command, args []string) error {
			key, err := ioutil.ReadAll(bytes.NewReader([]byte(args[0])))
			if err != nil {
				c.QuitToStdErr(err)
			}
			ctx := context.Background()
			vi, err := cl.immuClient.RawSafeGet(ctx, key)
			if err != nil {
				c.QuitToStdErr(err)
			}
			printItem(vi.Key, vi.Value, vi)
			return nil
		},
		Args: cobra.ExactArgs(1),
	}
	cmd.AddCommand(ccmd)
}

func (cl *commandline) safeGetKey(cmd *cobra.Command) {
	ccmd := &cobra.Command{
		Use:     "safeget key",
		Short:   "Get and verify item having the specified key",
		Aliases: []string{"sg"},
		RunE: func(cmd *cobra.Command, args []string) error {

			key, err := ioutil.ReadAll(bytes.NewReader([]byte(args[0])))
			if err != nil {
				c.QuitToStdErr(err)
			}
			ctx := context.Background()
			response, err := cl.immuClient.SafeGet(ctx, key)
			if err != nil {
				c.QuitWithUserError(err)
			}
			printItem([]byte(args[0]), nil, response)
			return nil
		},
		Args: cobra.ExactArgs(1),
	}
	cmd.AddCommand(ccmd)
}

package commands

import (
	"bytes"
	"context"
	"io/ioutil"
	"strconv"

	c "github.com/codenotary/immudb/cmd"
	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/codenotary/immudb/pkg/client"
	"github.com/codenotary/immudb/pkg/client/cache"
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
			immuClient := cl.getImmuClient(cmd)
			response, err := immuClient.Connected(ctx, func() (interface{}, error) {
				return immuClient.ByIndex(ctx, index)
			})
			if err != nil {
				c.QuitWithUserError(err)
			}
			printByIndex(response.(*schema.StructuredItem))
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
			immuClient := cl.getImmuClient(cmd)
			response, err := immuClient.Connected(ctx, func() (interface{}, error) {
				return immuClient.Get(ctx, key)
			})
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
			immuClient := cl.getImmuClient(cmd)
			rootService := client.NewRootService(immuClient.ServiceClient, cache.NewFileCache())
			root, err := rootService.GetRoot(ctx)
			if err != nil {
				c.QuitToStdErr(err)
			}

			sgOpts := &schema.SafeGetOptions{
				Key: key,
				RootIndex: &schema.Index{
					Index: root.Index,
				},
			}

			safeItem, err := immuClient.Connected(ctx, func() (interface{}, error) {
				return immuClient.ServiceClient.SafeGet(ctx, sgOpts)
			})
			if err != nil {
				c.QuitWithUserError(err)
			}
			si := safeItem.(*schema.SafeItem)
			h, err := si.Hash()
			if err != nil {
				c.QuitWithUserError(err)
			}
			verified := si.Proof.Verify(h, *root)
			if verified {
				// saving a fresh root
				tocache := new(schema.Root)
				tocache.Index = si.Proof.At
				tocache.Root = si.Proof.Root
				err := rootService.SetRoot(tocache)
				if err != nil {
					c.QuitWithUserError(err)
				}
			}

			vi := &client.VerifiedItem{
				Key:      si.Item.GetKey(),
				Value:    si.Item.Value,
				Index:    si.Item.GetIndex(),
				Verified: verified,
			}
			printItem(si.Item.GetKey(), si.Item.Value, vi)
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
			immuClient := cl.getImmuClient(cmd)
			response, err := immuClient.Connected(ctx, func() (interface{}, error) {
				return immuClient.SafeGet(ctx, key)
			})
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

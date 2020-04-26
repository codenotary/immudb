package commands

import (
	"bufio"
	"bytes"
	"context"
	"io"
	"io/ioutil"
	"os"

	c "github.com/codenotary/immudb/cmd"
	"github.com/spf13/cobra"
)

func (cl *commandline) newRefkeyKey(cmd *cobra.Command) {
	ccmd := &cobra.Command{
		Use:     "reference refkey key",
		Short:   "Add new reference to an existing key",
		Aliases: []string{"r"},
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
			immuClient := cl.getImmuClient(cmd)
			response, err := immuClient.Connected(ctx, func() (interface{}, error) {
				return immuClient.Reference(ctx, reference, key)
			})
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

func (cl *commandline) safeNewRefkeyKey(cmd *cobra.Command) {
	ccmd := &cobra.Command{
		Use:     "safereference refkey key",
		Short:   "Add and verify new reference to an existing key",
		Aliases: []string{"sr"},
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
			immuClient := cl.getImmuClient(cmd)
			response, err := immuClient.Connected(ctx, func() (interface{}, error) {
				return immuClient.SafeReference(ctx, reference, key)
			})
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

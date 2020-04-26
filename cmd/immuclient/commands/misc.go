package commands

import (
	"bytes"
	"context"
	"encoding/hex"
	"fmt"
	"io/ioutil"
	"os"
	"strconv"
	"time"

	c "github.com/codenotary/immudb/cmd"
	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/spf13/cobra"
)

func (cl *commandline) checkConsistencyIndexHash(cmd *cobra.Command) {
	ccmd := &cobra.Command{
		Use:     "check-consistency index hash",
		Short:   "Check consistency for the specified index and hash",
		Aliases: []string{"c"},
		RunE: func(cmd *cobra.Command, args []string) error {
			index, err := strconv.ParseUint(args[0], 10, 64)
			if err != nil {
				c.QuitToStdErr(err)
			}
			ctx := context.Background()
			immuClient := cl.getImmuClient(cmd)
			response, err := immuClient.Connected(ctx, func() (interface{}, error) {
				return immuClient.Consistency(ctx, index)
			})
			if err != nil {
				c.QuitWithUserError(err)
			}

			proof := response.(*schema.ConsistencyProof)

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

func (cl *commandline) historyKey(cmd *cobra.Command) {
	ccmd := &cobra.Command{
		Use:     "history key",
		Short:   "Fetch history for the item having the specified key",
		Aliases: []string{"h"},
		RunE: func(cmd *cobra.Command, args []string) error {

			key, err := ioutil.ReadAll(bytes.NewReader([]byte(args[0])))
			if err != nil {
				c.QuitToStdErr(err)
			}
			ctx := context.Background()
			immuClient := cl.getImmuClient(cmd)
			response, err := immuClient.Connected(ctx, func() (interface{}, error) {
				return immuClient.History(ctx, key)
			})
			if err != nil {
				c.QuitWithUserError(err)
			}
			for _, item := range response.(*schema.StructuredItemList).Items {
				printItem(nil, nil, item)
				fmt.Println()
			}
			return nil
		},
		Args: cobra.ExactArgs(1),
	}
	cmd.AddCommand(ccmd)
}

func (cl *commandline) ping(cmd *cobra.Command) {
	ccmd := &cobra.Command{
		Use:     "ping",
		Short:   "Ping to check if server connection is alive",
		Aliases: []string{"p"},
		RunE: func(cmd *cobra.Command, args []string) error {

			ctx := context.Background()
			immuClient := cl.getImmuClient(cmd)
			_, err := immuClient.Connected(ctx, func() (interface{}, error) {
				return nil, immuClient.HealthCheck(ctx)
			})
			if err != nil {
				c.QuitWithUserError(err)
			}
			fmt.Println("Health check OK")
			return nil
		},
		Args: cobra.NoArgs,
	}
	cmd.AddCommand(ccmd)
}

func (cl *commandline) dumpToFile(cmd *cobra.Command) {
	ccmd := &cobra.Command{
		Use:     "dump [filename]",
		Short:   "Save a database dump to the specified filename (optional)",
		Aliases: []string{"b"},
		RunE: func(cmd *cobra.Command, args []string) error {

			filename := fmt.Sprint("immudb_" + time.Now().Format("2006-01-02_15-04-05") + ".bkp")
			if len(args) > 0 {
				filename = args[0]
			}
			file, err := os.Create(filename)
			defer file.Close()
			if err != nil {
				c.QuitToStdErr(err)
			}
			ctx := context.Background()
			immuClient := cl.getImmuClient(cmd)
			response, err := immuClient.Connected(ctx, func() (interface{}, error) {
				return immuClient.Dump(ctx, file)
			})
			if err != nil {
				c.QuitWithUserError(err)
			}
			fmt.Printf("SUCCESS: %d key-value entries were backed-up to file %s\n", response.(int64), filename)
			return nil
		},
		Args: cobra.MaximumNArgs(1),
	}
	cmd.AddCommand(ccmd)
}

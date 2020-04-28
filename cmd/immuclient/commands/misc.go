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

func (cl *commandline) consistency(cmd *cobra.Command) {
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
			proof, err := cl.immuClient.Consistency(ctx, index)
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

func (cl *commandline) history(cmd *cobra.Command) {
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
			response, err := cl.immuClient.History(ctx, key)
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

func (cl *commandline) healthCheck(cmd *cobra.Command) {
	ccmd := &cobra.Command{
		Use:     "ping",
		Short:   "Ping to check if server connection is alive",
		Aliases: []string{"p"},
		RunE: func(cmd *cobra.Command, args []string) error {

			ctx := context.Background()
			if err := cl.immuClient.HealthCheck(ctx); err != nil {
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
			response, err := cl.immuClient.Dump(ctx, file)
			if err != nil {
				c.QuitWithUserError(err)
			}
			fmt.Printf("SUCCESS: %d key-value entries were backed-up to file %s\n", response, filename)
			return nil
		},
		Args: cobra.MaximumNArgs(1),
	}
	cmd.AddCommand(ccmd)
}

func (cl *commandline) inclusion(cmd *cobra.Command) {
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

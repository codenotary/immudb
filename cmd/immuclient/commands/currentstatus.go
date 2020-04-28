package commands

import (
	"context"

	c "github.com/codenotary/immudb/cmd"
	"github.com/spf13/cobra"
)

func (cl *commandline) currentRoot(cmd *cobra.Command) {
	ccmd := &cobra.Command{
		Use:     "current",
		Short:   "Return the last merkle tree root and index stored locally",
		Aliases: []string{"crt"},
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := context.Background()
			root, err := cl.immuClient.CurrentRoot(ctx)
			if err != nil {
				c.QuitWithUserError(err)
			}
			printRoot(root)
			return nil
		},
		Args: cobra.ExactArgs(0),
	}
	cmd.AddCommand(ccmd)
}

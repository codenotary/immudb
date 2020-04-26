package commands

import (
	"context"

	c "github.com/codenotary/immudb/cmd"
	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/spf13/cobra"
)

func (cl *commandline) currentStatus(cmd *cobra.Command) {
	cccmd := &cobra.Command{
		Use:     "current",
		Short:   "Return the last merkle tree root and index stored locally",
		Aliases: []string{"crt"},
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := context.Background()
			immuClient := cl.getImmuClient(cmd)
			root, err := immuClient.Connected(ctx, func() (interface{}, error) {
				return immuClient.CurrentRoot(ctx)
			})
			if err != nil {
				c.QuitWithUserError(err)
			}
			printRoot(root.(*schema.Root))
			return nil
		},
		Args: cobra.ExactArgs(0),
	}
	cmd.AddCommand(cccmd)
}

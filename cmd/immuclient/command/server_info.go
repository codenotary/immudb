package immuclient

import "github.com/spf13/cobra"

func (cl *commandline) serverInfo(cmd *cobra.Command) {
	ccmd := &cobra.Command{
		Use:               "info",
		Short:             "Return server information",
		PersistentPreRunE: cl.ConfigChain(cl.connect),
		PersistentPostRun: cl.disconnect,
		RunE: func(cmd *cobra.Command, args []string) error {
			resp, err := cl.immucl.ServerInfo(args)
			if err != nil {
				cl.quit(err)
			}
			fprintln(cmd.OutOrStdout(), resp)
			return nil
		},
		Args: cobra.ExactArgs(0),
	}
	cmd.AddCommand(ccmd)
}

/*
Copyright 2019-2020 vChain, Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

	http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package immuclient

import (
	"fmt"

	c "github.com/codenotary/immudb/cmd/helper"
	"github.com/codenotary/immudb/cmd/immuclient/audit"
	"github.com/codenotary/immudb/cmd/immuclient/cli"
	"github.com/codenotary/immudb/cmd/immuclient/service"
	"github.com/spf13/cobra"
)

func (cl *commandline) history(cmd *cobra.Command) {
	ccmd := &cobra.Command{
		Use:               "history key",
		Short:             "Fetch history for the item having the specified key",
		Aliases:           []string{"h"},
		PersistentPreRunE: cl.connect,
		PersistentPostRun: cl.disconnect,
		RunE: func(cmd *cobra.Command, args []string) error {
			resp, err := cl.immucl.History(args)
			if err != nil {
				c.QuitToStdErr(err)
			}
			fmt.Println(resp)
			return nil
		},
		Args: cobra.ExactArgs(1),
	}
	cmd.AddCommand(ccmd)
}

func (cl *commandline) status(cmd *cobra.Command) {
	ccmd := &cobra.Command{
		Use:               "status",
		Short:             "Ping to check if server connection is alive",
		Aliases:           []string{"p"},
		PersistentPreRunE: cl.connect,
		PersistentPostRun: cl.disconnect,
		RunE: func(cmd *cobra.Command, args []string) error {
			resp, err := cl.immucl.HealthCheck(args)
			if err != nil {
				c.QuitToStdErr(err)
			}
			fmt.Println(resp)
			return nil
		},
		Args: cobra.NoArgs,
	}
	cmd.AddCommand(ccmd)
}

func (cl *commandline) auditmode(cmd *cobra.Command) {
	ccmd := &cobra.Command{
		Use:               "audit-mode command",
		Short:             "Starts immuclient as daemon in auditor mode. Run 'immuclient audit-mode help' or use -h flag for details",
		Aliases:           []string{"audit-mode"},
		Example:           service.UsageExamples,
		PersistentPostRun: cl.disconnect,
		ValidArgs:         []string{"help", "start", "install", "uninstall", "restart", "stop", "status"},
		RunE: func(cmd *cobra.Command, args []string) error {
			audit.Init(args)
			return nil
		},
		Args: cobra.MaximumNArgs(2),
	}
	cmd.AddCommand(ccmd)
}

// #TODO will be new root.
func (cl *commandline) interactiveCli(cmd *cobra.Command) {
	ccmd := &cobra.Command{
		Use:     "it",
		Short:   "Starts immuclient in CLI mode. Use 'help' or -h flag on the shell for details",
		Aliases: []string{"cli-mode"},
		Example: cli.Init(cl.immucl).HelpMessage(),
		RunE: func(cmd *cobra.Command, args []string) error {
			cli.Init(cl.immucl).Run()
			return nil
		},
	}
	cmd.AddCommand(ccmd)
}

func (cl *commandline) user(cmd *cobra.Command) {
	ccmd := &cobra.Command{
		Use:               "user command",
		Short:             "Issue all user commands",
		Aliases:           []string{"u"},
		PersistentPreRunE: cl.connect,
		PersistentPostRun: cl.disconnect,
		ValidArgs:         []string{"help", "list", "create", "permission grant", "permission revoke", "change password", "activate", "deactivate"},
		RunE: func(cmd *cobra.Command, args []string) error {
			resp, err := cl.immucl.UserOperations(args)
			if err != nil {
				c.QuitToStdErr(err)
			}
			fmt.Println(resp)
			return nil
		},
		Args: cobra.MaximumNArgs(5),
	}
	cmd.AddCommand(ccmd)
}
func (cl *commandline) database(cmd *cobra.Command) {
	ccmd := &cobra.Command{
		Use:               "database command",
		Short:             "Issue all user commands",
		Aliases:           []string{"d"},
		PersistentPreRunE: cl.connect,
		PersistentPostRun: cl.disconnect,
		ValidArgs:         []string{"help", "list", "create databasename"},
		RunE: func(cmd *cobra.Command, args []string) error {
			resp, err := cl.immucl.CreateDatabase(args)
			if err != nil {
				c.QuitToStdErr(err)
			}
			fmt.Println(resp)
			return nil
		},
		Args: cobra.MaximumNArgs(3),
	}
	cmd.AddCommand(ccmd)
}
func (cl *commandline) use(cmd *cobra.Command) {
	ccmd := &cobra.Command{
		Use:               "use command",
		Short:             "select database",
		PersistentPreRunE: cl.connect,
		PersistentPostRun: cl.disconnect,
		ValidArgs:         []string{"databasename"},
		RunE: func(cmd *cobra.Command, args []string) error {
			resp, err := cl.immucl.UseDatabase(args)
			if err != nil {
				c.QuitToStdErr(err)
			}
			fmt.Println(resp)
			return nil
		},
		Args: cobra.MaximumNArgs(2),
	}
	cmd.AddCommand(ccmd)
}

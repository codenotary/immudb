/*
Copyright 2022 Codenotary Inc. All rights reserved.

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

package immuadmin

import (
	"github.com/c-bata/go-prompt"
	"github.com/google/shlex"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
)

func (cl *commandline) shell(cmd *cobra.Command) {
	scmd := &cobra.Command{
		Use:               "shell",
		Aliases:           []string{},
		PersistentPreRunE: cl.ConfigChain(cl.connect),
		RunE: func(cmd *cobra.Command, args []string) error {
			// Hide the shell command so that it does not show up in any
			// help message for interactive commands.
			cmd.Hidden = true
			// Store the non interactive setting of the command line and set it
			// to false. The setting is restored after the shell exits.
			nonInteractive := cl.nonInteractive
			cl.nonInteractive = false
			defer func() { cl.nonInteractive = nonInteractive }()
			// Create a new shell and run it.
			shell := NewImmuShell(cmd.Parent())
			shell.run()
			return nil
		},
		Args: cobra.NoArgs,
	}
	cmd.AddCommand(scmd)
}

// NewImmuShell adjusts the given root command and creates a shell using it.
func NewImmuShell(root *cobra.Command) *immuShell {
	// Store the persistent flags of the root command.
	rootFlags := make(map[*pflag.Flag]bool)
	root.PersistentFlags().VisitAll(
		func(flag *pflag.Flag) {
			rootFlags[flag] = true
			// Also hide the flag, as it should only be set on startup.
			flag.Hidden = true
		})
	// Do not print the usage text on every error.
	root.SilenceUsage = true
	// Create a new shell an store the PersistentPostRun function of the root
	// command.
	shell := &immuShell{
		root:                  root,
		rootFlags:             rootFlags,
		rootPersistentPostRun: root.PersistentPostRun,
	}
	// Remove the persistent pre and post run functions of the root command.
	// These would connect and disconnect from the immudb instance for each
	// individual command, which is not necessary.
	root.PersistentPreRunE = nil
	root.PersistentPostRun = nil
	// Remove all persistent post and pre runs of children for the same reason.
	for _, cmd := range root.Commands() {
		cmd.PersistentPreRunE = nil
		cmd.PersistentPostRun = nil
	}
	// Reinitialize help for the shell.
	root.InitDefaultHelpCmd()
	root.InitDefaultHelpFlag()
	return shell
}

// immuShell creates a shell for the root command using go-prompt.
type immuShell struct {
	root                  *cobra.Command
	rootFlags             map[*pflag.Flag]bool
	rootPersistentPostRun func(*cobra.Command, []string)
}

// run the interactive shell.
func (is *immuShell) run() {
	// Create a command prompt and run it.
	p := prompt.New(
		is.execute,
		is.suggest,
		prompt.OptionTitle("immuadmin"),
		prompt.OptionPrefix("immuadmin>"),
	)
	p.Run()
	// Restore the persistent post run function of the root command,
	// so that it can execute and perform clean up.
	is.root.PersistentPostRun = is.rootPersistentPostRun
}

// execute the command denoted by the input line and arguments.
func (is *immuShell) execute(line string) {
	// Extract args from the input line and set them as input args for cobra.
	args, err := shlex.Split(line)
	if err != nil {
		// Abort if the command line  arguments could not be parsed.
		is.root.ErrOrStderr().Write([]byte(err.Error()))
		return
	}
	is.root.SetArgs(args)
	// Clear all flag values except for persistent flags of the root command
	// for the command which will be executed by cobra.
	if cmd, _, err := is.root.Find(args); err == nil {
		cmd.Flags().VisitAll(func(flag *pflag.Flag) {
			// Skip the flag if it is a flag of the root command.
			if _, ok := is.rootFlags[flag]; ok {
				return
			}
			// Check if the flag stores a slice.
			if flagValue, ok := flag.Value.(pflag.SliceValue); ok {
				// Reset the value by setting an empty slice.
				_ = flagValue.Replace([]string{})
			} else {
				// Reset to the default value of the flag.
				_ = flag.Value.Set(flag.DefValue)
			}
		})
	}
	// Finally execute the command.
	is.root.Execute()
}

// suggest a command to run from the current input state.
func (is *immuShell) suggest(d prompt.Document) []prompt.Suggest {
	// For not no suggestions are made.
	return []prompt.Suggest{}
}

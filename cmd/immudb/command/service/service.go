/*
Copyright 2024 Codenotary Inc. All rights reserved.

SPDX-License-Identifier: BUSL-1.1
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    https://mariadb.com/bsl11/

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package service

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"time"

	"github.com/codenotary/immudb/cmd/immudb/command/service/constants"

	"github.com/spf13/cobra"
	daem "github.com/takama/daemon"
)

var availableCommands = []string{"install", "uninstall", "start", "stop", "restart", "status"}

func (cl *commandline) Service(cmd *cobra.Command) {
	ccmd := &cobra.Command{
		Use:   fmt.Sprintf("service %v", availableCommands),
		Short: "Immudb service management tool",
		Long: `Manage immudb service.
Root permission are required in order to make administrator operations.
Currently working on linux, windows and freebsd operating systems.
`,
		ValidArgs: availableCommands,
		Example:   constants.UsageExamples,
		Args: func(cmd *cobra.Command, args []string) error {
			if len(args) < 1 {
				return errors.New("required a command name")
			}
			if !stringInSlice(args[0], availableCommands) {
				return fmt.Errorf("invalid command argument specified: %s. Available list is %v", args[0], availableCommands)
			}
			return nil
		},
		RunE: func(cmd *cobra.Command, args []string) (err error) {

			if ok, e := cl.sservice.IsAdmin(); !ok {
				return e
			}
			// delayed operation
			t, _ := cmd.Flags().GetInt("time")
			if t > 0 {
				// if t is present we relaunch same command with --delayed flag set
				var argi []string
				for i, k := range os.Args {
					if k == "--time" || k == "-t" {
						continue
					}
					if _, err = strconv.ParseFloat(k, 64); err == nil {
						continue
					}
					if i != 0 {
						argi = append(argi, k)
					}
				}
				argi = append(argi, "--delayed", strconv.Itoa(t))
				if err = launch(os.Args[0], argi); err != nil {
					return err
				}
				return nil
			}
			// if delayed flag is set we delay the execution of the action
			d, _ := cmd.Flags().GetInt("delayed")
			if d > 0 {
				time.Sleep(time.Duration(d) * time.Second)
			}

			var msg string

			daemon, err := cl.sservice.NewDaemon("immudb", "immudb - the immutable database")
			if err != nil {
				return err
			}

			var u string
			switch args[0] {
			case "install":
				if err = cl.sservice.InstallSetup("immudb", cmd.Parent()); err != nil {
					return err
				}
				var cp string
				if cp, err = cl.sservice.GetDefaultConfigPath("immudb"); err != nil {
					return err
				}
				if msg, err = daemon.Install("--config", cp); err != nil {
					return err
				}
				fmt.Fprintf(cmd.OutOrStdout(), msg+"\n")

				if msg, err = daemon.Start(); err != nil {
					return err
				}
				fmt.Fprintf(cmd.OutOrStdout(), msg+"\n")

				return nil
			case "uninstall":
				// check if already installed
				var status string
				if status, err = daemon.Status(); err != nil {
					if err == daem.ErrNotInstalled {
						return err
					}
				}
				fmt.Fprintf(cmd.OutOrStdout(), "Are you sure you want to uninstall %s? [y/N]", "immudb")
				if u, err = cl.treader.ReadFromTerminalYN("N"); err != nil {
					return err
				}
				if u != "y" {
					fmt.Fprintf(cmd.OutOrStdout(), msg+"\n")
					return
				}
				// stopping service first
				if cl.sservice.IsRunning(status) {
					if msg, err = daemon.Stop(); err != nil {
						return err
					}
					fmt.Fprintf(cmd.OutOrStdout(), msg+"\n")
				}
				if msg, err = daemon.Remove(); err != nil {
					return err
				}
				fmt.Fprintf(cmd.OutOrStdout(), msg+"\n")

				fmt.Fprintf(cmd.OutOrStdout(), "Erase data? [y/N]")
				if u, err = cl.treader.ReadFromTerminalYN("N"); err != nil {
					return err
				}
				if u != "y" {
					fmt.Fprintf(cmd.OutOrStdout(), "No data removed\n")
				} else {
					if err = cl.sservice.EraseData("immudb"); err != nil {
						return err
					}
					fmt.Fprintf(cmd.OutOrStdout(), "Data folder removed\n")
				}
				if err = cl.sservice.UninstallSetup("immudb"); err != nil {
					return err
				}
				fmt.Fprintf(cmd.OutOrStdout(), "Program files removed\n")

				return nil
			case "start":
				if msg, err = daemon.Start(); err != nil {
					return err
				}
				fmt.Fprintf(cmd.OutOrStdout(), msg+"\n")
				return nil
			case "stop":
				if msg, err = daemon.Stop(); err != nil {
					return err
				}
				fmt.Fprintf(cmd.OutOrStdout(), msg+"\n")
				return nil
			case "restart":
				if _, err = daemon.Stop(); err != nil {
					return err
				}
				fmt.Fprintf(cmd.OutOrStdout(), msg+"\n")
				if msg, err = daemon.Start(); err != nil {
					return err
				}
				fmt.Fprintf(cmd.OutOrStdout(), msg+"\n")
				return nil
			case "status":
				if msg, err = daemon.Status(); err != nil {
					return err
				}
				fmt.Fprintf(cmd.OutOrStdout(), msg+"\n")
				return nil
			}
			return nil
		},
	}
	ccmd.PersistentFlags().IntP("time", "t", 0, "number of seconds to wait before stopping | restarting the service")
	ccmd.PersistentFlags().Int("delayed", 0, "number of seconds to wait before repeat the parent command. HIDDEN")
	ccmd.PersistentFlags().MarkHidden("delayed")
	cmd.AddCommand(ccmd)
}

func stringInSlice(a string, list []string) bool {
	for _, b := range list {
		if b == a {
			return true
		}
	}
	return false
}

func launch(command string, args []string) (err error) {
	cmd := exec.Command(command, args...)
	if err = cmd.Start(); err != nil {
		return err
	}
	return nil
}

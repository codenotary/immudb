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

package commands

import (
	"errors"
	"fmt"
	"github.com/spf13/cobra"
	"github.com/takama/daemon"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"time"
)

var (
	// ErrUnsupportedSystem appears if try to use service on system which is not supported by this release
	ErrUnsupportedSystem = errors.New("Unsupported system")

	// ErrRootPrivileges appears if run installation or deleting the service without root privileges
	ErrRootPrivileges = errors.New("You must have root user privileges. Possibly using 'sudo' command should help")

	ErrRootUserNeeded = errors.New("You must be logged as root user")
)

var installable_services = []string{"immudb", "immugw"}
var available_commands = []string{"install", "uninstall", "start", "stop", "restart", "status"}

func (cl *commandlineDisc) service(cmd *cobra.Command) {
	ccmd := &cobra.Command{
		Use:           fmt.Sprintf("service %v %v", installable_services, available_commands),
		Short:         "Manage immu services",
		SilenceUsage:  true,
		SilenceErrors: true,
		ValidArgs:     available_commands,
		Example: `
sudo ./immuadmin service immudb install --local-file vchain/immudb/src/immudb
immuadmin service immudb install --local-file immudb.exe
sudo ./immuadmin service immudb uninstall
`,
		Args: func(cmd *cobra.Command, args []string) error {
			if len(args) < 1 {
				return errors.New("Required a service name")
			}
			if stringInSlice("--remove-files", os.Args) {
				return nil
			}
			if len(args) < 2 {
				return errors.New("Required a command name")
			}
			if !stringInSlice(args[0], installable_services) {
				return fmt.Errorf("invalid service argument specified: %s. Available list is %v.", args[0], installable_services)
			}
			if !stringInSlice(args[1], available_commands) {
				return fmt.Errorf("invalid command argument specified: %s. Available list is %v.", args[1], available_commands)
			}
			return nil
		},
		RunE: func(cmd *cobra.Command, args []string) (err error) {

			if ok, err := checkPrivileges(); !ok {
				return err
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
					if _, err := strconv.ParseFloat(k, 64); err == nil {
						continue
					}
					if i != 0 {
						argi = append(argi, k)
					}
				}
				argi = append(argi, "--delayed")
				argi = append(argi, strconv.Itoa(t))
				launch(os.Args[0], argi)
				return nil
			}
			// if delayed flag is set we delay the execution of the action
			d, _ := cmd.Flags().GetInt("delayed")
			if d > 0 {
				time.Sleep(time.Duration(d) * time.Second)
			}

			var msg string
			var localFile string

			if stringInSlice("--remove-files", os.Args) || args[1] == "install" {
				if localFile, err = cmd.Flags().GetString("local-file"); err != nil {
					return err
				}
				if localFile != "" {
					_, err = os.Stat(localFile)
					if os.IsNotExist(err) {
						return errors.New("Provided executable file does not exists")
					}
				}

				if localFile == "" {
					localFile = args[0]
					if runtime.GOOS == "windows" {
						localFile = localFile + ".exe"
					}
					_, err = os.Stat(localFile)
					if os.IsNotExist(err) {
						return fmt.Errorf("%s executable file was not found on current folder", args[0])
					}
				}

				if localFile, err = filepath.Abs(localFile); err != nil {
					return err
				}
			}

			remove, _ := cmd.Flags().GetBool("remove-files")
			if remove {
				os.Remove(localFile)
				return nil
			}

			daemon, _ := daemon.New(args[0], args[0], localFile)

			switch args[1] {
			case "install":
				fmt.Println("installing " + localFile + "...")
				if msg, err = daemon.Install(); err != nil {
					return err
				}
				fmt.Println(msg)
				return nil
			case "uninstall":
				var u string
				fmt.Printf("Are you sure you want to uninstall %s? Default N [Y/N]", args[0])
				n, _ := fmt.Scanln(&u)
				if n <= 0 {
					u = "N"
				}
				u = strings.TrimSpace(strings.ToLower(u))
				if u == "y" {
					if msg, err = daemon.Remove(); err != nil {
						return err
					}
					fmt.Println(msg)
				} else {
					fmt.Println("No action.")
				}
				return nil
			case "start":
				if msg, err = daemon.Start(); err != nil {
					return err
				}
				fmt.Println(msg)
				return nil
			case "stop":
				if msg, err = daemon.Stop(); err != nil {
					return err
				}
				fmt.Println(msg)
				return nil
			case "restart":
				if _, err = daemon.Stop(); err != nil {
					return err
				}
				fmt.Println(msg)
				if msg, err = daemon.Start(); err != nil {
					return err
				}
				fmt.Println(msg)
				return nil
			case "status":
				if msg, err = daemon.Status(); err != nil {
					return err
				}
				fmt.Println(msg)
				return nil
			}

			return nil
		},
	}

	ccmd.PersistentFlags().Bool("remove-files", false, "clean up from all service files")
	ccmd.PersistentFlags().IntP("time", "t", 0, "number of seconds to wait before stopping | restarting the service")
	ccmd.PersistentFlags().Int("delayed", 0, "number of seconds to wait before repeat the parent command. HIDDEN")
	ccmd.PersistentFlags().MarkHidden("delayed")
	ccmd.PersistentFlags().String("local-file", "", "local executable file name")
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
	if err := cmd.Start(); err != nil {
		return err
	}
	return nil
}

// Check root rights to use system service
func checkPrivileges() (bool, error) {
	if output, err := exec.Command("id", "-g").Output(); err == nil {
		if gid, parseErr := strconv.ParseUint(strings.TrimSpace(string(output)), 10, 32); parseErr == nil {
			if gid == 0 {
				return true, nil
			}
			return false, ErrRootPrivileges
		}
	}
	return false, ErrUnsupportedSystem
}

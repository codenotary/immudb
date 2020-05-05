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
	daem "github.com/takama/daemon"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"time"
)

var (
	// errUnsupportedSystem appears if try to use service on system which is not supported by this release
	errUnsupportedSystem = errors.New("unsupported system")

	// errRootPrivileges appears if run installation or deleting the service without root privileges
	errRootPrivileges = errors.New("you must have root user privileges. Possibly using 'sudo' command should help")

	// errExecNotFound provided executable file does not exists
	errExecNotFound = errors.New("provided executable file does not exists")
)

var installableServices = []string{"immudb", "immugw"}
var availableCommands = []string{"install", "uninstall", "start", "stop", "restart", "status"}

func (cl *commandlineDisc) service(cmd *cobra.Command) {
	ccmd := &cobra.Command{
		Use:   fmt.Sprintf("service %v %v", installableServices, availableCommands),
		Short: "Manage immu services",
		Long: `Manage immudb related services.
Configuration service installation in /etc/immudb is available only under non windows os.
Currently installing the service under windows may incur anomalies. Related issues on https://github.com/takama/daemon/issues/68.
Installable services are immudb and immugw.
Root permission are required in order to make administrator operations.
`,
		ValidArgs: availableCommands,
		Example: `
sudo ./immuadmin service immudb install --local-file vchain/immudb/src/immudb
immuadmin service immudb install --local-file immudb.exe
sudo ./immuadmin service immudb uninstall
`,
		Args: func(cmd *cobra.Command, args []string) error {
			if len(args) < 1 {
				return errors.New("required a service name")
			}
			if stringInSlice("--remove-files", os.Args) {
				return nil
			}
			if len(args) < 2 {
				return errors.New("required a command name")
			}
			if !stringInSlice(args[0], installableServices) {
				return fmt.Errorf("invalid service argument specified: %s. Available list is %v", args[0], installableServices)
			}
			if !stringInSlice(args[1], availableCommands) {
				return fmt.Errorf("invalid command argument specified: %s. Available list is %v", args[1], availableCommands)
			}
			return nil
		},
		RunE: func(cmd *cobra.Command, args []string) (err error) {

			if ok, e := checkPrivileges(); !ok {
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
				argi = append(argi, "--delayed")
				argi = append(argi, strconv.Itoa(t))
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
			var localFile string

			if stringInSlice("--remove-files", os.Args) || args[1] == "install" {
				if localFile, err = cmd.Flags().GetString("local-file"); err != nil {
					return err
				}
				if localFile != "" {
					_, err = os.Stat(localFile)
					if os.IsNotExist(err) {
						return errExecNotFound
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
				if err = os.Remove(localFile); err != nil {
					return err
				}
				return nil
			}

			daemon, _ := daem.New(args[0], args[0], localFile)

			switch args[1] {
			case "install":
				fmt.Println("installing " + localFile + "...")
				if runtime.GOOS == "windows" {
					if err = installConfig(args[0]); err != nil {
						return err
					}
				}

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
					if runtime.GOOS == "windows" {
						if err = uninstallConfig(args[0]); err != nil {
							return err
						}
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

func installConfig(serviceName string) error {
	err := os.MkdirAll("/etc/"+serviceName, os.ModePerm)
	if err != nil {
		return err
	}
	from, err := os.Open("configs/immudb.ini.dist")
	if err != nil {
		return err
	}
	defer from.Close()

	to, err := os.OpenFile("/etc/"+serviceName+"/"+serviceName+".ini", os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return err
	}
	defer to.Close()

	_, err = io.Copy(to, from)
	if err != nil {
		return err
	}
	return nil
}

func uninstallConfig(serviceName string) error {
	err := os.Remove("/etc/" + serviceName + "/" + serviceName + ".ini")
	if err != nil {
		return err
	}
	return nil
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

func checkPrivileges() (bool, error) {
	if output, err := exec.Command("id", "-g").Output(); err == nil {
		if gid, parseErr := strconv.ParseUint(strings.TrimSpace(string(output)), 10, 32); parseErr == nil {
			if gid == 0 {
				return true, nil
			}
			return false, errRootPrivileges
		}
	}
	return false, errUnsupportedSystem
}

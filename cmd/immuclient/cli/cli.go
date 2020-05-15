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

package cli

import (
	"errors"
	"fmt"
	"io"
	"os"
	"runtime"
	"strings"

	c "github.com/codenotary/immudb/cmd/helper"
	"github.com/codenotary/immudb/pkg/client"
	"github.com/peterh/liner"
	"github.com/spf13/viper"
)

type cli struct {
	commands       map[string]*command
	ImmuClient     client.ImmuClient
	passwordReader c.PasswordReader
	commandsList   []*command
	helpMessage    string
	valueOnly      bool
	isLoggedin     bool
}

type Cli interface {
	Run()
}

func Init() (Cli, error) {
	cli := new(cli)
	cli.passwordReader = c.DefaultPasswordReader
	var err error
	parseflags()
	if cli.ImmuClient, err = client.NewImmuClient(options()); err != nil || cli.ImmuClient == nil {
		return nil, fmt.Errorf("Initialization failed: %s \n", err.Error())
	}
	cli.valueOnly = viper.GetBool("value-only")
	cli.commands = make(map[string]*command)
	cli.commandsList = make([]*command, 0)
	return cli, nil
}

func options() *client.Options {
	port := viper.GetInt("immudb-port")
	address := viper.GetString("immudb-address")
	tokenFileName := viper.GetString("tokenfile")
	mtls := viper.GetBool("mtls")
	certificate := viper.GetString("certificate")
	servername := viper.GetString("servername")
	pkey := viper.GetString("pkey")
	clientcas := viper.GetString("clientcas")
	options := client.DefaultOptions().
		WithPort(port).
		WithAddress(address).
		WithTokenFileName(tokenFileName).
		WithMTLs(mtls)
	if mtls {
		// todo https://golang.org/src/crypto/x509/root_linux.go
		options.MTLsOptions = client.DefaultMTLsOptions().
			WithServername(servername).
			WithCertificate(certificate).
			WithPkey(pkey).
			WithClientCAs(clientcas)
	}
	return options
}

func (cli *cli) Register(cmd *command) {
	cli.commandsList = append(cli.commandsList, cmd)
	cli.commands[cmd.name] = cmd
}

func (cli *cli) helpInit() {
	var namelen, shortlen int
	name := make([]string, 0)
	short := make([]string, 0)
	args := make([]string, 0)
	for i := range cli.commandsList {
		if len(cli.commandsList[i].name) > namelen {
			namelen = len(cli.commandsList[i].name)
		}
		if len(cli.commandsList[i].short) > shortlen {
			shortlen = len(cli.commandsList[i].short)
		}
		name = append(name, cli.commandsList[i].name)
		short = append(short, cli.commandsList[i].short)
		if len(cli.commandsList[i].args) == 0 {
			args = append(args, "")
		} else {
			args = append(args, strings.Join(cli.commandsList[i].args, ","))
		}
	}
	str := strings.Builder{}
	for i := range name {
		str.WriteString(padRight(name[i], " ", namelen+2))
		str.WriteString(padRight(short[i], " ", shortlen+2))
		if len(args[i]) > 0 {
			str.WriteString("args: " + args[i])
		}
		str.WriteString("\n")
	}
	str.WriteString("\n")
	cli.helpMessage = str.String()
}

func (cli *cli) Run() {
	cli.initCommands()
	cli.helpInit()
	l := liner.NewLiner()
	l.SetCompleter(cli.completer)
	defer l.Close()
	for {
		line, err := l.Prompt("immuclient>")
		if err == liner.ErrInvalidPrompt {
			if len(line) == 0 {
				break
			} else {
				continue
			}
		} else if err == io.EOF {
			continue
		}
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
		}
		l.AppendHistory(line)
		line = strings.TrimSuffix(line, "\n")
		arrCommandStr := strings.Fields(line)
		if len(arrCommandStr) == 0 {
			continue
		}
		passed := cli.checkCommand(arrCommandStr, l)
		if passed {
			cli.runCommand(arrCommandStr)
		}
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
		}
	}
}

func (cli *cli) checkCommand(arrCommandStr []string, l *liner.State) bool {
	if arrCommandStr[0] == "exit" || arrCommandStr[0] == "quit" {
		if cli.isLoggedin {
			logoutmsg, _ := cli.logout(nil)
			fmt.Println(logoutmsg)
		}
		l.Close()
		os.Exit(0)
	}
	switch arrCommandStr[0] {
	case "--help":
		fmt.Fprint(os.Stdout, cli.helpMessage)
		return false
	case "help":
		fmt.Fprint(os.Stdout, cli.helpMessage)
		return false
	case "-h":
		fmt.Fprint(os.Stdout, cli.helpMessage)
		return false
	case "clear":
		cleaner, ok := clear[runtime.GOOS]
		if !ok {
			fmt.Fprintf(os.Stdout, "ERROR: %s \n", "Current OS not supporting for this command.")
			return false
		}
		cleaner()
		return false
	}
	if len(arrCommandStr) == 2 && (arrCommandStr[1] == "--help" || arrCommandStr[1] == "-h") {
		helpline, err := cli.singleCommandHelp(arrCommandStr[0])
		if err != nil {
			suggestions := cli.correct(arrCommandStr[0])
			str := strings.Builder{}
			str.WriteString(fmt.Sprintf("ERROR: %s | %s  \n", "Command not found ", arrCommandStr[0]))
			if len(suggestions) != 0 {
				str.WriteString("Did you mean this ?\n")
				for i := range suggestions {
					str.WriteString(fmt.Sprintf("	%s \n", suggestions[i]))
				}
			}
			str.WriteString("Run --help for usage \n")
			fmt.Fprint(os.Stdout, str.String())
			return false
		}
		fmt.Fprintf(os.Stdout, "%v \n", helpline)
		return false
	}
	return true
}

func (cli *cli) runCommand(arrCommandStr []string) {
	command, ok := cli.commands[arrCommandStr[0]]
	if !ok {
		suggestions := cli.correct(arrCommandStr[0])
		str := strings.Builder{}
		str.WriteString(fmt.Sprintf("ERROR: %s | %s  \n", "Unknown command ", arrCommandStr[0]))
		if len(suggestions) != 0 {
			str.WriteString("\n")
			str.WriteString("Did you mean this ?\n")
			for i := range suggestions {
				str.WriteString(fmt.Sprintf("	%s \n", suggestions[i]))
			}
		}
		str.WriteString("\n")
		str.WriteString("Run --help for usage \n")
		fmt.Fprint(os.Stdout, str.String())
		return
	}
	if len(arrCommandStr[1:]) < len(command.args) {
		fmt.Fprintf(os.Stdout,
			"ERROR: Not enough arguments | %s needs %v , have %v . Use [command] --help for documentation. \n",
			command.name,
			len(command.args),
			len(arrCommandStr[1:]))
		return
	}
	valOnly := false
	if len(arrCommandStr[1:]) > len(command.args) {
		redunantArgs := make([]string, 0)
		excessargs := arrCommandStr[len(command.args):]
		for i := 1; i < len(excessargs); i++ {
			if !strings.HasPrefix(excessargs[i], "-") {
				redunantArgs = append(redunantArgs, excessargs[i])
			} else {
				if excessargs[i] == "--value-only" && !cli.valueOnly {
					valOnly = true
				}
			}
		}
		if len(redunantArgs) > 0 {
			fmt.Fprintf(os.Stdout,
				"INFO: Redunant argument(s) | %v \n", redunantArgs)
		}
	}
	if valOnly {
		cli.valueOnly = true
	}
	result, err := command.command(arrCommandStr[1:])
	if valOnly {
		cli.valueOnly = false
	}
	if err != nil {
		fmt.Fprintf(os.Stdout, "ERROR: %s \n", err.Error())
		return
	} else {
		fmt.Fprintf(os.Stdout, "%v \n", result)
		return
	}
}

func (cli *cli) singleCommandHelp(cmdName string) (string, error) {
	cmd, ok := cli.commands[cmdName]
	if !ok {
		return "", errors.New("not found")
	}
	args := ""
	if len(cmd.args) > 0 {
		args = strings.Join(cmd.args, ",")
	}
	return fmt.Sprintf("%s %s args:%s", cmd.name, cmd.short, args), nil
}

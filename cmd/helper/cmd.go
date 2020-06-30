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

package helper

// todo @Michele command package can be refactored in order to semplify packages schema

import (
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"text/tabwriter"
	"time"

	"golang.org/x/crypto/ssh/terminal"

	"github.com/mitchellh/go-homedir"
	"github.com/spf13/viper"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// DetachedFlag ...
const DetachedFlag = "detached"

// DetachedShortFlag ...
const DetachedShortFlag = "d"

// LinuxManPath ...
const LinuxManPath = "/usr/share/man/man1/"

// Options cmd options
type Options struct {
	CfgFn string
}

// InitConfig initializes config
func (o *Options) InitConfig(name string) {
	if o.CfgFn != "" {
		viper.SetConfigFile(o.CfgFn)
	} else {
		home, err := homedir.Dir()
		if err != nil {
			QuitToStdErr(err)
		}
		viper.AddConfigPath("configs")
		viper.AddConfigPath(os.Getenv("GOPATH") + "/src/configs")
		if runtime.GOOS != "windows" {
			viper.AddConfigPath("/etc/immudb")
		}
		viper.AddConfigPath(home)
		viper.SetConfigName(name)
	}
	viper.SetEnvPrefix(strings.ToUpper(name))
	viper.SetEnvKeyReplacer(strings.NewReplacer("-", "_"))
	viper.AutomaticEnv()
	if err := viper.ReadInConfig(); err == nil {
		o.CfgFn = viper.ConfigFileUsed()
		fmt.Println("Using config file:", o.CfgFn)
	}
}

// Detached launch command in background
func Detached() {
	var err error
	var executable string
	var args []string

	if executable, err = os.Executable(); err != nil {
		QuitToStdErr(err)
	}

	for i, k := range os.Args {
		if k != "--"+DetachedFlag && k != "-"+DetachedShortFlag && i != 0 {
			args = append(args, k)
		}
	}

	cmd := exec.Command(executable, args...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	if err = cmd.Start(); err != nil {
		QuitToStdErr(err)
	}
	time.Sleep(1 * time.Second)
	fmt.Fprintf(
		os.Stdout, "%s%s has been started with %sPID %d%s\n",
		Green, filepath.Base(executable), Blue, cmd.Process.Pid, Reset)
	os.Exit(0)
}

// QuitToStdErr prints an error on stderr and closes
func QuitToStdErr(msg interface{}) {
	_, _ = fmt.Fprintln(os.Stderr, msg)
	os.Exit(1)
}

// QuitWithUserError ...
func QuitWithUserError(err error) {
	s, ok := status.FromError(err)
	if !ok {
		QuitToStdErr(err)
	}
	if s.Code() == codes.Unauthenticated {
		QuitToStdErr(errors.New("unauthenticated, please login"))
	}
	QuitToStdErr(err)
}

// PasswordReader ...
type PasswordReader interface {
	Read(string) ([]byte, error)
}
type stdinPasswordReader struct{}

func (pr *stdinPasswordReader) Read(msg string) ([]byte, error) {
	fmt.Print(msg)
	pass, err := terminal.ReadPassword(int(os.Stdin.Fd()))
	fmt.Println()
	if err != nil {
		return nil, err
	}
	return pass, nil
}

// DefaultPasswordReader ...
var DefaultPasswordReader PasswordReader = new(stdinPasswordReader)

// ReadFromTerminalYN read terminal user input from a Yes No dialog. It returns y and n only with an explicit Yy or Nn input. If no input is submitted it returns default value. If the input is different from the expected one empty string is returned.
func ReadFromTerminalYN(def string) (selected string, err error) {
	var u string
	var n int
	if n, err = fmt.Scanln(&u); err != nil && err != io.EOF && err.Error() != "unexpected newline" {
		return "", err
	}
	if n <= 0 {
		u = def
	}
	u = strings.TrimSpace(strings.ToLower(u))
	if u == "y" {
		return "y", nil
	}
	if u == "n" {
		return "n", nil
	}
	return "", nil
}

// UsageSprintf ...
func UsageSprintf(usages map[string][]string) string {
	subCmds := make([]string, 0, len(usages))
	for subCmd := range usages {
		subCmds = append(subCmds, subCmd)
	}
	sort.Strings(subCmds)
	var usagesBuilder strings.Builder
	for i, subCmd := range subCmds {
		descAndUsage := usages[subCmd]
		usagesBuilder.WriteString("  ")
		usagesBuilder.WriteString(descAndUsage[0])
		usagesBuilder.WriteString(":\n    ")
		usagesBuilder.WriteString(descAndUsage[1])
		if i < len(subCmds)-1 {
			usagesBuilder.WriteString("\n")
		}
	}
	return usagesBuilder.String()
}

// RequiredArgs ...
type RequiredArgs struct {
	Cmd    string
	Usage  string
	Usages map[string][]string
}

// Require ...
func (ra *RequiredArgs) Require(
	args []string,
	argPos int,
	argName string,
	invalidArgName string,
	validValues map[string]struct{},
	action string,
) (string, error) {
	isValidAction := true
	if action != "" {
		_, isValidAction = ra.Usages[action]
		if !isValidAction {
			action = ""
		}
	}
	usage := ra.Usage
	if action != "" {
		usage = ra.Usages[action][1]
	}
	if len(args) < argPos+1 || !isValidAction {
		return "", fmt.Errorf(
			"Please specify %s.\nUsage: %s\nHelp : %s -h", argName, usage, ra.Cmd)
	}
	if len(validValues) > 0 {
		if _, validValue := validValues[args[argPos]]; !validValue {
			validValuesArr := make([]string, 0, len(validValues))
			for v := range validValues {
				validValuesArr = append(validValuesArr, v)
			}
			return "", fmt.Errorf(
				"Please specify %s: %s.\nUsage: %s\nHelp : %s -h",
				invalidArgName, strings.Join(validValuesArr, "|"), usage, ra.Cmd)
		}
	}
	return args[argPos], nil
}

// PrintTable ...
func PrintTable(cols []string, nbRows int, getRow func(int) []string) {
	if nbRows == 0 {
		return
	}
	nbCols := len(cols)
	if nbCols == 0 {
		return
	}
	colSep := "\t"

	maxNbDigits := 0
	tens := nbRows
	for tens != 0 {
		tens /= 10
		maxNbDigits++
	}
	header := append([]string{strings.Repeat("#", maxNbDigits)}, cols...)

	var sb strings.Builder
	for _, th := range header {
		for i := 0; i < len(th); i++ {
			sb.WriteString("-")
		}
		sb.WriteString(colSep)
	}
	borderBottom := sb.String()
	sb.Reset()

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, borderBottom)
	fmt.Fprint(w, strings.Join(header, colSep), colSep, "\n")
	fmt.Fprintln(w, borderBottom)
	for i := 0; i < nbRows; i++ {
		row := getRow(i)
		nbRowCols := len(row)
		for j := 0; j < nbCols; j++ {
			if j < nbRowCols {
				sb.WriteString(row[j])
			}
			sb.WriteString(colSep)
		}
		fmt.Fprint(w, strconv.Itoa(i+1), colSep, sb.String(), "\n")
		sb.Reset()
	}
	fmt.Fprintln(w, borderBottom)
	_ = w.Flush()
}

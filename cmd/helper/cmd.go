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
	"runtime"
	"strings"

	"golang.org/x/crypto/ssh/terminal"

	"github.com/mitchellh/go-homedir"
	"github.com/spf13/viper"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const DetachedFlag = "detached"
const DetachedShortFlag = "d"

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
	viper.AutomaticEnv()
	if err := viper.ReadInConfig(); err == nil {
		fmt.Println("Using config file:", viper.ConfigFileUsed())
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

	if err = cmd.Start(); err != nil {
		QuitToStdErr(err)
	}
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

/*
Copyright 2022 CodeNotary, Inc. All rights reserved.

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

package immudb

import (
	"bytes"
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/codenotary/immudb/cmd/immudb/command/immudbcmdtest"
	"github.com/codenotary/immudb/pkg/server"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
	"github.com/codenotary/immudb/cmd/helper"
)

func DefaultTestOptions() (o *server.Options) {
	o = server.DefaultOptions()
	o.Pidfile = "tmp/immudbtest/immudbtest.pid"
	o.Logfile = "immudbtest.log"
	o.Dir = "tmp/immudbtest/data"
	return o
}

func TestImmudbCommandFlagParser(t *testing.T) {
	o := DefaultTestOptions()

	var options *server.Options
	var err error
	cmd := &cobra.Command{
		Use: "immudb",
		RunE: func(cmd *cobra.Command, args []string) (err error) {
			options, err = parseOptions()
			if err != nil {
				return err
			}
			return nil
		},
	}
	cl := Commandline{}
	cl.setupFlags(cmd, server.DefaultOptions())

	err = viper.BindPFlags(cmd.Flags())
	assert.Nil(t, err)

	setupDefaults(server.DefaultOptions())

	_, err = executeCommand(cmd, "--logfile="+o.Logfile)
	assert.NoError(t, err)
	assert.Equal(t, o.Logfile, options.Logfile)
}

func TestImmudbCommandFlagParserWrongTLS(t *testing.T) {
	viper.Set("mtls", true)
	o := DefaultTestOptions()

	var err error
	cmd := &cobra.Command{
		Use: "immudb",
		RunE: func(cmd *cobra.Command, args []string) (err error) {
			_, err = parseOptions()
			if err != nil {
				return err
			}
			return nil
		},
	}
	cl := Commandline{}
	cl.setupFlags(cmd, server.DefaultOptions())

	err = viper.BindPFlags(cmd.Flags())
	assert.Nil(t, err)

	setupDefaults(server.DefaultOptions())

	_, err = executeCommand(cmd, "--logfile="+o.Logfile)

	assert.Error(t, err)
	viper.Set("mtls", false)
}



//Priority:
// 1. overrides
// 2. flags
// 3. env. variables
// 4. config file
func TestImmudbCommandFlagParserPriority(t *testing.T) {
	defer tearDown()
	o := DefaultTestOptions()
	var options *server.Options
	var err error
	cl := Commandline{}
	cl.config.Name = "immudb"

	cmd := &cobra.Command{
		Use:               "immudb",
		PersistentPreRunE: cl.ConfigChain(nil),
		RunE: func(cmd *cobra.Command, args []string) (err error) {
			options, err = parseOptions()
			if err != nil {
				return err
			}
			return nil
		},
	}
	cl.setupFlags(cmd, server.DefaultOptions())

	err = viper.BindPFlags(cmd.Flags())
	assert.Nil(t, err)

	setupDefaults(server.DefaultOptions())

	// 4. config file
	_, err = executeCommand(cmd)
	assert.NoError(t, err)
	assert.Equal(t, "", options.Logfile)
	// 4-b. config file specified in command line
	_, err = executeCommand(cmd, "--config=../../../test/immudb.toml")
	assert.NoError(t, err)
	assert.Equal(t, "ConfigFileThatsNameIsDeclaredOnTheCommandLine", options.Logfile)

	// 3. env. variables
	os.Setenv("IMMUDB_LOGFILE", "EnvironmentVars")
	_, err = executeCommand(cmd)
	assert.NoError(t, err)
	assert.Equal(t, "EnvironmentVars", options.Logfile)

	// 2. flags
	_, err = executeCommand(cmd, "--logfile="+o.Logfile)
	assert.NoError(t, err)
	assert.Equal(t, o.Logfile, options.Logfile)

	// 1. overrides
	viper.Set("logfile", "override")
	_, err = executeCommand(cmd, "--logfile="+o.Logfile)
	assert.NoError(t, err)
	assert.Equal(t, "override", options.Logfile)
}

func executeCommand(root *cobra.Command, args ...string) (output string, err error) {
	_, output, err = executeCommandC(root, args...)
	return output, err
}

func executeCommandC(root *cobra.Command, args ...string) (c *cobra.Command, output string, err error) {
	buf := new(bytes.Buffer)
	root.SetOut(buf)
	root.SetErr(buf)
	root.SetArgs(args)
	c, err = root.ExecuteC()
	return c, buf.String(), err
}

func tearDown() {
	os.Unsetenv("IMMUDB_LOGFILE")
}

func TestImmudb(t *testing.T) {
	var config string
	cmd := &cobra.Command{}
	cmd.Flags().StringVar(&config, "config", "", "test")

	cl := Commandline{}

	immudb := cl.Immudb(immudbcmdtest.ImmuServerMock{})
	err := immudb(cmd, nil)
	assert.Nil(t, err)

}

func TestImmudbDetached(t *testing.T) {
	var config string
	cmd := &cobra.Command{}
	cmd.Flags().StringVar(&config, "config", "", "test")
	viper.Set("detached", true)

	cl := Commandline{P: plauncherMock{}}

	immudb := cl.Immudb(immudbcmdtest.ImmuServerMock{})
	err := immudb(cmd, nil)
	assert.Nil(t, err)
	viper.Set("detached", false)
}

func TestImmudbMtls(t *testing.T) {
	var config string
	cmd := &cobra.Command{}
	cmd.Flags().StringVar(&config, "config", "", "test")
	viper.Set("mtls", true)
	viper.Set("pkey", "../../../test/mtls_certs/ca.key.pem")
	viper.Set("certificate", "../../../test/mtls_certs/ca.cert.pem")
	viper.Set("clientcas", "../../../test/mtls_certs/ca-chain.cert.pem")

	cl := Commandline{}

	immudb := cl.Immudb(immudbcmdtest.ImmuServerMock{})
	err := immudb(cmd, nil)
	assert.Nil(t, err)
	viper.Set("mtls", false)
}

func TestImmudbLogFile(t *testing.T) {
	var config string
	cmd := &cobra.Command{}
	cmd.Flags().StringVar(&config, "config", "", "test")
	viper.Set("logfile", "override")
	defer os.Remove("override")

	cl := Commandline{}

	immudb := cl.Immudb(immudbcmdtest.ImmuServerMock{})
	err := immudb(cmd, nil)
	assert.Nil(t, err)
}

type plauncherMock struct{}

func (pl plauncherMock) Detached() error {
	return nil
}

func TestNewCommand(t *testing.T) {
	_, err := newCommand(server.DefaultServer())
	require.NoError(t, err)
}

func TestExecute(t * testing.T) {
	quitCode := 0
	os.Setenv("IMMUDB_ADDRESS","999.999.999.999")
	helper.OverrideQuitter(func(q int) {
		quitCode=q
	})
	Execute()
	assert.Equal(t, quitCode,1)
}

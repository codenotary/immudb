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

package immudb

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/codenotary/immudb/cmd/helper"
	"github.com/codenotary/immudb/cmd/immudb/command/immudbcmdtest"
	"github.com/codenotary/immudb/pkg/server"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
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
	require.NoError(t, err)

	setupDefaults(server.DefaultOptions())

	_, err = executeCommand(cmd, "--logfile="+o.Logfile)
	require.NoError(t, err)
	require.Equal(t, o.Logfile, options.Logfile)
}

func TestImmudbCommandFlagParserWrongTLS(t *testing.T) {
	defer viper.Reset()

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
	require.NoError(t, err)

	setupDefaults(server.DefaultOptions())

	_, err = executeCommand(cmd, "--logfile="+o.Logfile)

	require.Error(t, err)
}

// Priority:
// 1. overrides
// 2. flags
// 3. env. variables
// 4. config file
func TestImmudbCommandFlagParserPriority(t *testing.T) {
	defer viper.Reset()

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
	require.NoError(t, err)

	setupDefaults(server.DefaultOptions())

	// 4. config file
	_, err = executeCommand(cmd)
	require.NoError(t, err)
	require.Equal(t, "", options.Logfile)
	// 4-b. config file specified in command line
	_, err = executeCommand(cmd, "--config=../../../test/immudb.toml")
	require.NoError(t, err)
	require.Equal(t, "ConfigFileThatsNameIsDeclaredOnTheCommandLine", options.Logfile)

	// 3. env. variables
	t.Setenv("IMMUDB_LOGFILE", "EnvironmentVars")
	_, err = executeCommand(cmd)
	require.NoError(t, err)
	require.Equal(t, "EnvironmentVars", options.Logfile)

	// 2. flags
	_, err = executeCommand(cmd, "--logfile="+o.Logfile)
	require.NoError(t, err)
	require.Equal(t, o.Logfile, options.Logfile)

	// 1. overrides
	viper.Set("logfile", "override")
	_, err = executeCommand(cmd, "--logfile="+o.Logfile)
	require.NoError(t, err)
	require.Equal(t, "override", options.Logfile)
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

func TestImmudb(t *testing.T) {
	var config string
	cmd := &cobra.Command{}
	cmd.Flags().StringVar(&config, "config", "", "test")
	setupDefaults(server.DefaultOptions())

	cl := Commandline{}

	immudb := cl.Immudb(&immudbcmdtest.ImmuServerMock{})
	err := immudb(cmd, nil)
	require.NoError(t, err)

}

func TestImmudbDetached(t *testing.T) {
	defer viper.Reset()

	var config string
	cmd := &cobra.Command{}
	cmd.Flags().StringVar(&config, "config", "", "test")
	setupDefaults(server.DefaultOptions())

	viper.Set("detached", true)

	cl := Commandline{P: plauncherMock{}}

	immudb := cl.Immudb(&immudbcmdtest.ImmuServerMock{})
	err := immudb(cmd, nil)
	require.NoError(t, err)
}

func TestImmudbMtls(t *testing.T) {
	defer viper.Reset()

	var config string
	cmd := &cobra.Command{}
	cmd.Flags().StringVar(&config, "config", "", "test")
	setupDefaults(server.DefaultOptions())

	viper.Set("mtls", true)
	viper.Set("pkey", "../../../test/mtls_certs/ca.key.pem")
	viper.Set("certificate", "../../../test/mtls_certs/ca.cert.pem")
	viper.Set("clientcas", "../../../test/mtls_certs/ca-chain.cert.pem")

	cl := Commandline{}

	immudb := cl.Immudb(&immudbcmdtest.ImmuServerMock{})
	err := immudb(cmd, nil)
	require.NoError(t, err)
}

func TestImmudbLogFile(t *testing.T) {
	defer viper.Reset()

	var config string
	cmd := &cobra.Command{}
	cmd.Flags().StringVar(&config, "config", "", "test")
	setupDefaults(server.DefaultOptions())

	viper.Set("dir", t.TempDir())
	viper.Set("logfile", "override")

	cl := Commandline{}

	immudb := cl.Immudb(&immudbcmdtest.ImmuServerMock{})
	err := immudb(cmd, nil)
	require.NoError(t, err)
}

type plauncherMock struct{}

func (pl plauncherMock) Detached() error {
	return nil
}

func TestNewCommand(t *testing.T) {
	_, err := newCommand(server.DefaultServer())
	require.NoError(t, err)
}

func TestExecute(t *testing.T) {
	quitCode := 0
	t.Setenv("IMMUDB_ADDRESS", "999.999.999.999")
	t.Setenv("IMMUDB_DIR", t.TempDir())
	helper.OverrideQuitter(func(q int) {
		quitCode = q
	})
	Execute()
	require.Equal(t, quitCode, 1)
}

func TestImmudbCommandReplicationFlagsParser(t *testing.T) {
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
	require.NoError(t, err)

	setupDefaults(server.DefaultOptions())

	_, err = executeCommand(cmd, "--replication-is-replica")
	require.NoError(t, err)
	require.True(t, options.ReplicationOptions.IsReplica)
}

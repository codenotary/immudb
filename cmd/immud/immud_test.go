package main

import (
	"bytes"
	"github.com/codenotary/immudb/pkg/server"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

func DefaultTestOptions() (o server.Options) {
	o = server.DefaultOptions()
	o.Pidpath = "tmp/immudtest/immudtest.pid"
	o.Logpath = "immudtest.log"
	o.Dir = "tmp/immudtest/data"
	o.DbName = "immudtest"
	o.MTLs = false
	return o
}

func TestImmudCommandFlagParser(t *testing.T) {
	o := DefaultTestOptions()

	var options server.Options
	var err error
	cmd := &cobra.Command{
		Use: "immud",
		RunE: func(cmd *cobra.Command, args []string) (err error) {
			options, err = parseOptions(cmd)
			if err != nil {
				return err
			}
			return nil
		},
	}
	setupFlags(cmd, server.DefaultOptions(), server.DefaultMTLsOptions())
	bindFlags(cmd)
	setupDefaults(server.DefaultOptions(), server.DefaultMTLsOptions())

	_, err = executeCommand(cmd, "--logpath="+o.Logpath)
	assert.NoError(t, err)
	assert.Equal(t, o.Logpath, options.Logpath)
}

//Priority:
// 1. overrides
// 2. flags
// 3. env. variables
// 4. config file
func TestImmudCommandFlagParserPriority(t *testing.T) {
	o := DefaultTestOptions()
	var options server.Options
	var err error
	cmd := &cobra.Command{
		Use: "immud",
		RunE: func(cmd *cobra.Command, args []string) (err error) {
			options, err = parseOptions(cmd)
			if err != nil {
				return err
			}
			return nil
		},
	}
	setupFlags(cmd, server.DefaultOptions(), server.DefaultMTLsOptions())
	bindFlags(cmd)
	setupDefaults(server.DefaultOptions(), server.DefaultMTLsOptions())

	// 4. config file
	_, err = executeCommand(cmd)
	assert.NoError(t, err)
	assert.Equal(t, "", options.Logpath)
	// 4-b. config file specified in command line
	_, err = executeCommand(cmd, "--cfgfile=./../../test/immucfgtest.toml")
	assert.NoError(t, err)
	assert.Equal(t, "ConfigFileThatsNameIsDeclaredOnTheCommandLine", options.Logpath)

	// 3. env. variables
	os.Setenv("IMMU_LOGPATH", "EnvironmentVars")
	_, err = executeCommand(cmd)
	assert.NoError(t, err)
	assert.Equal(t, "EnvironmentVars", options.Logpath)

	// 2. flags
	_, err = executeCommand(cmd, "--logpath="+o.Logpath)
	assert.NoError(t, err)
	assert.Equal(t, o.Logpath, options.Logpath)

	// 1. overrides
	viper.Set("logpath", "override")
	_, err = executeCommand(cmd, "--logpath="+o.Logpath)
	assert.NoError(t, err)
	assert.Equal(t, "override", options.Logpath)
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

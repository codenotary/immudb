package immuclient

import (
	"encoding/json"
	"fmt"

	c "github.com/codenotary/immudb/cmd/helper"
	"github.com/codenotary/immudb/cmd/immuclient/immuc"
	"github.com/codenotary/immudb/pkg/client"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

type commandline struct {
	immucl         immuc.Client
	config         c.Config
	onError        func(msg interface{})
	options        *immuc.Options
	outputRenderer func(immuc.CommandOutput, *cobra.Command) error
}

func NewCommandLine() commandline {
	cl := commandline{}
	cl.config.Name = "immuclient"
	cl.options = &immuc.Options{}
	cl.options.WithImmudbClientOptions(client.DefaultOptions())
	cl.outputRenderer = cl.renderOutputPlain
	return cl
}

func (cl *commandline) ConfigChain(post func(cmd *cobra.Command, args []string) error) func(cmd *cobra.Command, args []string) (err error) {
	return func(cmd *cobra.Command, args []string) (err error) {
		if err = cl.config.LoadConfig(cmd); err != nil {
			return err
		}

		if viper.GetBool("value-only") {
			cl.outputRenderer = cl.renderOutputValueOnly
		}

		switch viper.GetString("output") {
		case "plain":
			cl.outputRenderer = cl.renderOutputPlain
		case "value-only":
			cl.outputRenderer = cl.renderOutputValueOnly
		case "json":
			cl.outputRenderer = cl.renderOutputJson
		case "":
			// Do nothing
		default:
			return fmt.Errorf("Invalid output format: '%s', available options: 'plain', 'value-only', 'json'", viper.GetString("format"))
		}

		cl.options = immuc.OptionsFromEnv()
		cl.options.GetImmudbClientOptions().WithTokenFileName("token")
		cl.immucl, err = immuc.Init(cl.options)
		if err != nil {
			return err
		}
		if post != nil {
			return post(cmd, args)
		}
		return nil
	}
}

// Register ...
func (cl *commandline) Register(rootCmd *cobra.Command) *cobra.Command {
	// login and logout
	cl.login(rootCmd)
	cl.logout(rootCmd)
	// current status
	cl.health(rootCmd)
	cl.currentState(rootCmd)
	// get operations
	cl.getTxByID(rootCmd)
	cl.safegetTxByID(rootCmd)
	cl.getKey(rootCmd)
	cl.safeGetKey(rootCmd)
	// set operations
	cl.set(rootCmd)
	cl.safeset(rootCmd)
	cl.restore(rootCmd)
	cl.deleteKey(rootCmd)
	cl.zAdd(rootCmd)
	cl.safeZAdd(rootCmd)
	// scanners
	cl.zScan(rootCmd)
	cl.scan(rootCmd)
	cl.count(rootCmd)
	// references
	cl.reference(rootCmd)
	cl.safereference(rootCmd)
	// misc
	cl.consistency(rootCmd)
	cl.history(rootCmd)
	cl.status(rootCmd)
	cl.auditmode(rootCmd)
	cl.interactiveCli(rootCmd)
	cl.use(rootCmd)

	cl.sqlExec(rootCmd)
	cl.sqlQuery(rootCmd)
	cl.listTables(rootCmd)
	cl.describeTable(rootCmd)

	return rootCmd
}

func (cl *commandline) connect(cmd *cobra.Command, args []string) (err error) {
	err = cl.immucl.Connect(args)
	if err != nil {
		cl.quit(err)
	}
	return
}

func (cl *commandline) disconnect(cmd *cobra.Command, args []string) {
	if err := cl.immucl.Disconnect(args); err != nil {
		cl.quit(err)
	}
}

func (cl *commandline) quit(msg interface{}) error {
	if cl.onError == nil {
		c.QuitToStdErr(msg)
	}
	cl.onError(msg)
	return nil
}

func (cl *commandline) renderOutputPlain(resp immuc.CommandOutput, cmd *cobra.Command) error {
	fmt.Fprintf(cmd.OutOrStdout(), "%s\n", resp.Plain())
	return nil
}

func (cl *commandline) renderOutputValueOnly(resp immuc.CommandOutput, cmd *cobra.Command) error {
	fmt.Fprintf(cmd.OutOrStdout(), "%s\n", resp.ValueOnly())
	return nil
}

func (cl *commandline) renderOutputJson(resp immuc.CommandOutput, cmd *cobra.Command) error {
	enc := json.NewEncoder(cmd.OutOrStdout())
	enc.SetIndent("", " ")
	err := enc.Encode(resp.Json())
	if err != nil {
		return cl.quit(fmt.Sprintf("ERROR: Failed to output json data: %v\n", err))
	}
	return nil
}

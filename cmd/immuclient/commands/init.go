package commands

import (
	c "github.com/codenotary/immudb/cmd"
	"github.com/codenotary/immudb/cmd/docs/man"
	"github.com/codenotary/immudb/pkg/client"
	"github.com/codenotary/immudb/pkg/gw"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

type commandline struct {
	tokenFilename *string
}

func Init(cmd *cobra.Command, o *c.Options, tokenFilename *string) {
	cl := new(commandline)
	cl.tokenFilename = tokenFilename
	if err := configureOptions(cmd, o); err != nil {
		c.QuitToStdErr(err)
	}
	// login and logout
	cl.login(cmd)
	cl.logout(cmd)
	// current status
	cl.currentStatus(cmd)
	// get operations
	cl.getByIndex(cmd)
	cl.getKey(cmd)
	cl.rawSafeGetKey(cmd)
	cl.safeGetKey(cmd)
	// set operations
	cl.rawSafeSetKey(cmd)
	cl.setKeyValue(cmd)
	cl.safeSetKeyValue(cmd)
	cl.zAddSetNameScoreKey(cmd)
	cl.safeZAddSetNameScoreKey(cmd)
	// scanners
	cl.zScanSetName(cmd)
	cl.iScanPageNumPageSize(cmd)
	cl.iScanPrefix(cmd)
	cl.countPrefix(cmd)
	cl.inclusionIndex(cmd)
	// references
	cl.newRefkeyKey(cmd)
	cl.safeNewRefkeyKey(cmd)
	// misc
	cl.checkConsistencyIndexHash(cmd)
	cl.historyKey(cmd)
	cl.ping(cmd)
	cl.dumpToFile(cmd)

	// man file generator
	cmd.AddCommand(man.Generate(cmd, "immuclient", "../docs/man/immuclient"))
}

func configureOptions(cmd *cobra.Command, o *c.Options) error {
	cmd.PersistentFlags().IntP("port", "p", gw.DefaultOptions().ImmudbPort, "immudb port number")
	cmd.PersistentFlags().StringP("address", "a", gw.DefaultOptions().ImmudbAddress, "immudb host address")
	cmd.PersistentFlags().StringVar(&o.CfgFn, "config", "", "config file (default path are configs or $HOME. Default filename is immuclient.ini)")
	cmd.PersistentFlags().BoolP("auth", "s", client.DefaultOptions().Auth, "use authentication")
	if err := viper.BindPFlag("default.port", cmd.PersistentFlags().Lookup("port")); err != nil {
		return err
	}
	if err := viper.BindPFlag("default.address", cmd.PersistentFlags().Lookup("address")); err != nil {
		return err
	}
	if err := viper.BindPFlag("default.auth", cmd.PersistentFlags().Lookup("auth")); err != nil {
		return err
	}
	viper.SetDefault("default.port", gw.DefaultOptions().ImmudbPort)
	viper.SetDefault("default.address", gw.DefaultOptions().ImmudbAddress)
	viper.SetDefault("default.auth", client.DefaultOptions().Auth)
	return nil
}

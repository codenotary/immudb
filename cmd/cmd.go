package cmd

import (
	"fmt"
	"github.com/mitchellh/go-homedir"
	"github.com/spf13/viper"
	"os"
	"strings"
)

// Options cmd options
type Options struct {
	CfgFn string
}

// InitConfig initializes config
func (o Options) InitConfig(name string) {
	if o.CfgFn != "" {
		viper.SetConfigFile(o.CfgFn)
	} else {
		home, err := homedir.Dir()
		if err != nil {
			QuitToStdErr(err)
		}
		viper.AddConfigPath("configs")
		viper.AddConfigPath(os.Getenv("GOPATH") + "/src/configs")
		viper.AddConfigPath(home)
		viper.SetConfigName(name)
	}
	viper.SetEnvPrefix(strings.ToUpper(name))
	viper.AutomaticEnv()
	if err := viper.ReadInConfig(); err == nil {
		fmt.Println("Using config file:", viper.ConfigFileUsed())
	}
}

// QuitToStdErr prints an error on stderr and closes
func QuitToStdErr(msg interface{}) {
	_, _ = fmt.Fprintln(os.Stderr, msg)
	os.Exit(1)
}

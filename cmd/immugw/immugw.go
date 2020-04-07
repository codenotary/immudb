package main

import (
	"fmt"
	"github.com/codenotary/immudb/pkg/logger"
	"github.com/mitchellh/go-homedir"
	"github.com/spf13/viper"
	"os"

	"github.com/codenotary/immudb/cmd/docs/man"
	"github.com/codenotary/immudb/pkg/client"
	"github.com/codenotary/immudb/pkg/gw"
	"github.com/spf13/cobra"
)

var (
	config string
)

func init() {
	cobra.OnInitialize(initConfig)
}

func main() {

	immugwCmd := &cobra.Command{
		Use:   "immugw",
		Short: "Immu gateway",
		Long:  `Immu gateway is an smart proxy for immudb. It exposes all gRPC methods with a rest interface and wrap all SAFE endpoints with a verification service.`,
		RunE: func(cmd *cobra.Command, args []string) (err error) {
			var options gw.Options
			if options, err = parseOptions(cmd); err != nil {
				return err
			}
			immuGwServer := gw.
				DefaultServer().
				WithOptions(options)
			if options.Logfile != "" {
				if flogger, file, err := logger.NewFileLogger("immugw ", options.Logfile); err == nil {
					defer func() {
						if err := file.Close(); err != nil {
							quitToStdErr(err)
						}
					}()
					immuGwServer.WithLogger(flogger)
				} else {
					return err
				}
			}
			return immuGwServer.Start()
		},
	}

	setupFlags(immugwCmd, gw.DefaultOptions(), client.DefaultMTLsOptions())

	if err := bindFlags(immugwCmd); err != nil {
		quitToStdErr(err)
	}
	setupDefaults(gw.DefaultOptions(), client.DefaultMTLsOptions())
	immugwCmd.AddCommand(man.Generate(immugwCmd, "immugw", "../docs/man/immugw"))

	if err := immugwCmd.Execute(); err != nil {
		_, _ = fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func parseOptions(cmd *cobra.Command) (options gw.Options, err error) {
	port := viper.GetInt("port")
	address := viper.GetString("address")
	immudport := viper.GetInt("immudport")
	immudAddress := viper.GetString("immudAddress")
	// config file came only from arguments or default folder
	if config, err = cmd.Flags().GetString("config"); err != nil {
		return gw.Options{}, err
	}
	pidfile := viper.GetString("pidfile")
	logfile := viper.GetString("logfile")
	mtls := viper.GetBool("mtls")
	servername := viper.GetString("servername")
	certificate := viper.GetString("certificate")
	pkey := viper.GetString("pkey")
	client_cas := viper.GetString("clientcas")

	options = gw.DefaultOptions().
		WithPort(port).
		WithAddress(address).
		WithImmudAddress(immudAddress).
		WithImmudPort(immudport).
		WithPidfile(pidfile).
		WithLogfile(logfile).
		WithMTLs(mtls)
	if mtls {
		// todo https://golang.org/src/crypto/x509/root_linux.go
		options.MTLsOptions = client.DefaultMTLsOptions().
			WithServername(servername).
			WithCertificate(certificate).
			WithPkey(pkey).
			WithClientCAs(client_cas)
	}
	return options, nil
}

func setupFlags(cmd *cobra.Command, options gw.Options, mtlsOptions client.MTLsOptions) {
	cmd.Flags().IntP("port", "p", options.Port, "immugw port number")
	cmd.Flags().StringP("address", "a", options.Address, "immugw host address")
	cmd.Flags().IntP("immudport", "j", options.ImmudPort, "immudb port number")
	cmd.Flags().StringP("immudaddress", "k", options.ImmudAddress, "immudb host address")
	cmd.Flags().StringVar(&config, "config", "", "config file (default path are config or $HOME. Default filename is immugw.toml)")
	cmd.Flags().String("pidfile", options.Pidfile, "pid path with filename. E.g. /var/run/immugw.pid")
	cmd.Flags().String("logfile", options.Logfile, "log path with filename. E.g. /tmp/immugw/immugw.log")
	cmd.Flags().BoolP("mtls", "m", options.MTLs, "enable mutual tls")
	cmd.Flags().String("servername", mtlsOptions.Servername, "used to verify the hostname on the returned certificates")
	cmd.Flags().String("certificate", mtlsOptions.Certificate, "server certificate file path")
	cmd.Flags().String("pkey", mtlsOptions.Pkey, "server private key path")
	cmd.Flags().String("clientcas", mtlsOptions.ClientCAs, "clients certificates list. Aka certificate authority")
}

func bindFlags(cmd *cobra.Command) error {
	if err := viper.BindPFlag("port", cmd.Flags().Lookup("port")); err != nil {
		return err
	}
	if err := viper.BindPFlag("address", cmd.Flags().Lookup("address")); err != nil {
		return err
	}
	if err := viper.BindPFlag("immudport", cmd.Flags().Lookup("immudport")); err != nil {
		return err
	}
	if err := viper.BindPFlag("immudaddress", cmd.Flags().Lookup("immudaddress")); err != nil {
		return err
	}
	if err := viper.BindPFlag("pidfile", cmd.Flags().Lookup("pidfile")); err != nil {
		return err
	}
	if err := viper.BindPFlag("logfile", cmd.Flags().Lookup("logfile")); err != nil {
		return err
	}
	if err := viper.BindPFlag("mtls", cmd.Flags().Lookup("mtls")); err != nil {
		return err
	}
	if err := viper.BindPFlag("servername", cmd.Flags().Lookup("servername")); err != nil {
		return err
	}
	if err := viper.BindPFlag("certificate", cmd.Flags().Lookup("certificate")); err != nil {
		return err
	}
	if err := viper.BindPFlag("pkey", cmd.Flags().Lookup("pkey")); err != nil {
		return err
	}
	if err := viper.BindPFlag("clientcas", cmd.Flags().Lookup("clientcas")); err != nil {
		return err
	}
	return nil
}

func setupDefaults(options gw.Options, mtlsOptions client.MTLsOptions) {
	viper.SetDefault("port", options.Port)
	viper.SetDefault("address", options.Address)
	viper.SetDefault("immudport", options.ImmudPort)
	viper.SetDefault("immudaddress", options.ImmudAddress)
	viper.SetDefault("pidfile", options.Pidfile)
	viper.SetDefault("logfile", options.Logfile)
	viper.SetDefault("mtls", options.MTLs)
	viper.SetDefault("certificate", mtlsOptions.Certificate)
	viper.SetDefault("pkey", mtlsOptions.Pkey)
	viper.SetDefault("clientcas", mtlsOptions.ClientCAs)
}

func initConfig() {
	if config != "" {
		viper.SetConfigFile(config)
	} else {
		home, err := homedir.Dir()
		if err != nil {
			quitToStdErr(err)
		}
		viper.AddConfigPath("configs")
		viper.AddConfigPath(os.Getenv("GOPATH") + "/src/configs")
		viper.AddConfigPath(home)
		viper.SetConfigName("immugw")
	}
	viper.SetEnvPrefix("IMMUGW")
	viper.AutomaticEnv()
	if err := viper.ReadInConfig(); err == nil {
		fmt.Println("Using config file:", viper.ConfigFileUsed())
	}
}

func quitToStdErr(msg interface{}) {
	_, _ = fmt.Fprintln(os.Stderr, msg)
	os.Exit(1)
}

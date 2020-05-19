package immugw

import (
	"os"
	"path/filepath"

	"github.com/codenotary/immudb/cmd/docs/man"
	c "github.com/codenotary/immudb/cmd/helper"
	"github.com/codenotary/immudb/cmd/version"
	"github.com/codenotary/immudb/pkg/client"
	"github.com/codenotary/immudb/pkg/gw"
	"github.com/codenotary/immudb/pkg/logger"
	"github.com/spf13/cobra"
	"github.com/spf13/cobra/doc"
	"github.com/spf13/viper"
	daem "github.com/takama/daemon"
)

var o = c.Options{}

func init() {
	cobra.OnInitialize(func() { o.InitConfig("immugw") })
}

func NewCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "immugw",
		Short: "immu gateway: a smart REST proxy for immudb - the lightweight, high-speed immutable database for systems and applications",
		Long: `immu gateway: a smart REST proxy for immudb - the lightweight, high-speed immutable database for systems and applications.
It exposes all gRPC methods with a REST interface while wrapping all SAFE endpoints with a verification service.

Environment variables:
  IMMUGW_ADDRESS=127.0.0.1
  IMMUGW_PORT=3323
  IMMUGW_IMMUDB-ADDRESS=127.0.0.1
  IMMUGW_IMMUDB-PORT=3322
  IMMUGW_DIR=.
  IMMUGW_PIDFILE=
  IMMUGW_LOGFILE=
  IMMUGW_DETACHED=false
  IMMUGW_MTLS=false
  IMMUGW_SERVERNAME=localhost
  IMMUGW_PKEY=./tools/mtls/4_client/private/localhost.key.pem
  IMMUGW_CERTIFICATE=./tools/mtls/4_client/certs/localhost.cert.pem
  IMMUGW_CLIENTCAS=./tools/mtls/2_intermediate/certs/ca-chain.cert.pem`,
		RunE: Immugw,
	}

	setupFlags(cmd, gw.DefaultOptions(), client.DefaultMTLsOptions())

	if err := bindFlags(cmd); err != nil {
		c.QuitToStdErr(err)
	}
	setupDefaults(gw.DefaultOptions(), client.DefaultMTLsOptions())
	cmd.AddCommand(man.Generate(cmd, "immugw", "./cmd/docs/man/immugw"))

	cmd.AddCommand(version.VersionCmd())

	return cmd
}

func Immugw(cmd *cobra.Command, args []string) (err error) {
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
				if err = file.Close(); err != nil {
					c.QuitToStdErr(err)
				}
			}()
			immuGwServer.WithLogger(flogger)
		} else {
			return err
		}
	}
	if options.Detached {
		c.Detached()
	}

	var d daem.Daemon
	if d, err = daem.New("immugw", "immugw", "immugw"); err != nil {
		c.QuitToStdErr(err)
	}

	service := gw.Service{
		ImmuGwServer: *immuGwServer,
	}

	d.Run(service)

	return
}
func parseOptions(cmd *cobra.Command) (options gw.Options, err error) {
	dir := viper.GetString("dir")
	port := viper.GetInt("port")
	address := viper.GetString("address")
	immudbport := viper.GetInt("immudb-port")
	immudbAddress := viper.GetString("immudb-address")
	// config file came only from arguments or default folder
	if o.CfgFn, err = cmd.Flags().GetString("config"); err != nil {
		return gw.Options{}, err
	}
	audit := viper.GetBool("audit")
	auditInterval := viper.GetDuration("audit-interval")
	auditUsername := viper.GetString("audit-username")
	auditPassword := viper.GetString("audit-password")
	pidfile := viper.GetString("pidfile")
	logfile := viper.GetString("logfile")
	mtls := viper.GetBool("mtls")
	detached := viper.GetBool("detached")
	servername := viper.GetString("servername")
	certificate := viper.GetString("certificate")
	pkey := viper.GetString("pkey")
	clientcas := viper.GetString("clientcas")

	options = gw.DefaultOptions().
		WithDir(dir).
		WithPort(port).
		WithAddress(address).
		WithImmudbAddress(immudbAddress).
		WithImmudbPort(immudbport).
		WithAudit(audit).
		WithAuditInterval(auditInterval).
		WithAuditUsername(auditUsername).
		WithAuditPassword(auditPassword).
		WithPidfile(pidfile).
		WithLogfile(logfile).
		WithMTLs(mtls).
		WithDetached(detached)
	if mtls {
		// todo https://golang.org/src/crypto/x509/root_linux.go
		options.MTLsOptions = client.DefaultMTLsOptions().
			WithServername(servername).
			WithCertificate(certificate).
			WithPkey(pkey).
			WithClientCAs(clientcas)
	}
	return options, nil
}

func setupFlags(cmd *cobra.Command, options gw.Options, mtlsOptions client.MTLsOptions) {
	cmd.Flags().String("dir", options.Dir, "program files folder")
	cmd.Flags().IntP("port", "p", options.Port, "immugw port number")
	cmd.Flags().StringP("address", "a", options.Address, "immugw host address")
	cmd.Flags().IntP("immudb-port", "j", options.ImmudbPort, "immudb port number")
	cmd.Flags().StringP("immudb-address", "k", options.ImmudbAddress, "immudb host address")
	cmd.Flags().StringVar(&o.CfgFn, "config", "", "config file (default path are configs or $HOME. Default filename is immugw.toml)")
	cmd.Flags().Bool("audit", options.Audit, "enable audit (continuously fetches latest root from server, checks consistency against a local root and saves the latest root locally)")
	cmd.Flags().Duration("audit-interval", options.AuditInterval, "audit interval")
	cmd.Flags().String("audit-username", options.AuditUsername, "username used by auditor to login to immudb")
	cmd.Flags().String("audit-password", options.AuditPassword, "password used by auditor to login to immudb")
	cmd.Flags().String("pidfile", options.Pidfile, "pid path with filename. E.g. /var/run/immugw.pid")
	cmd.Flags().String("logfile", options.Logfile, "log path with filename. E.g. /tmp/immugw/immugw.log")
	cmd.Flags().BoolP("mtls", "m", options.MTLs, "enable mutual tls")
	cmd.Flags().BoolP(c.DetachedFlag, c.DetachedShortFlag, options.Detached, "run immudb in background")
	cmd.Flags().String("servername", mtlsOptions.Servername, "used to verify the hostname on the returned certificates")
	cmd.Flags().String("certificate", mtlsOptions.Certificate, "server certificate file path")
	cmd.Flags().String("pkey", mtlsOptions.Pkey, "server private key path")
	cmd.Flags().String("clientcas", mtlsOptions.ClientCAs, "clients certificates list. Aka certificate authority")
}

func bindFlags(cmd *cobra.Command) error {
	if err := viper.BindPFlag("dir", cmd.Flags().Lookup("dir")); err != nil {
		return err
	}
	if err := viper.BindPFlag("port", cmd.Flags().Lookup("port")); err != nil {
		return err
	}
	if err := viper.BindPFlag("address", cmd.Flags().Lookup("address")); err != nil {
		return err
	}
	if err := viper.BindPFlag("immudb-port", cmd.Flags().Lookup("immudb-port")); err != nil {
		return err
	}
	if err := viper.BindPFlag("immudb-address", cmd.Flags().Lookup("immudb-address")); err != nil {
		return err
	}
	if err := viper.BindPFlag("audit", cmd.Flags().Lookup("audit")); err != nil {
		return err
	}
	if err := viper.BindPFlag("audit-interval", cmd.Flags().Lookup("audit-interval")); err != nil {
		return err
	}
	if err := viper.BindPFlag("audit-username", cmd.Flags().Lookup("audit-username")); err != nil {
		return err
	}
	if err := viper.BindPFlag("audit-password", cmd.Flags().Lookup("audit-password")); err != nil {
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
	if err := viper.BindPFlag("detached", cmd.Flags().Lookup("detached")); err != nil {
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
	viper.SetDefault("dir", options.Dir)
	viper.SetDefault("port", options.Port)
	viper.SetDefault("address", options.Address)
	viper.SetDefault("immudb-port", options.ImmudbPort)
	viper.SetDefault("immudb-address", options.ImmudbAddress)
	viper.SetDefault("audit", options.Audit)
	viper.SetDefault("audit-interval", options.AuditInterval)
	viper.SetDefault("audit-username", options.AuditUsername)
	viper.SetDefault("audit-password", options.AuditPassword)
	viper.SetDefault("pidfile", options.Pidfile)
	viper.SetDefault("logfile", options.Logfile)
	viper.SetDefault("mtls", options.MTLs)
	viper.SetDefault("detached", options.Detached)
	viper.SetDefault("certificate", mtlsOptions.Certificate)
	viper.SetDefault("pkey", mtlsOptions.Pkey)
	viper.SetDefault("clientcas", mtlsOptions.ClientCAs)
}

func InstallManPages() error {
	header := &doc.GenManHeader{
		Title:   "immugw",
		Section: "1",
		Source:  "Generated by immugw command",
	}
	dir := c.LinuxManPath

	_ = os.Mkdir(dir, os.ModePerm)
	err := doc.GenManTree(NewCmd(), header, dir)
	if err != nil {
		return err
	}
	return nil
}

func UnistallManPages() error {
	return os.Remove(filepath.Join(c.LinuxManPath, "immugw.1"))
}

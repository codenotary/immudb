/*
Copyright 2025 Codenotary Inc. All rights reserved.

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
	"time"

	c "github.com/codenotary/immudb/cmd/helper"
	"github.com/codenotary/immudb/embedded/logger"
	"github.com/codenotary/immudb/pkg/server"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
)

func (cl *Commandline) setupFlags(cmd *cobra.Command, options *server.Options) {
	cmd.Flags().String("dir", options.Dir, "data folder")
	cmd.Flags().IntP("port", "p", options.Port, "port number")
	cmd.Flags().StringP("address", "a", options.Address, "bind address")
	cmd.Flags().Bool("replication-enabled", false, "set systemdb and defaultdb as replica") // deprecated, use replication-is-replica instead
	cmd.Flags().Bool("replication-is-replica", false, "set systemdb and defaultdb as replica")
	cmd.Flags().Bool("replication-sync-enabled", false, "enable synchronous replication")
	cmd.Flags().Int("replication-sync-acks", 0, "set a minimum number of replica acknowledgements required before transactions can be committed")
	cmd.Flags().String("replication-primary-host", "", "primary database host (if replica=true)")
	cmd.Flags().Int("replication-primary-port", 3322, "primary database port (if replica=true)")
	cmd.Flags().String("replication-primary-username", "", "username in the primary database used for replication of systemdb and defaultdb")
	cmd.Flags().String("replication-primary-password", "", "password in the primary database used for replication of systemdb and defaultdb")
	cmd.Flags().Int("replication-prefetch-tx-buffer-size", options.ReplicationOptions.PrefetchTxBufferSize, "maximum number of prefeched transactions")
	cmd.Flags().Int("replication-commit-concurrency", options.ReplicationOptions.ReplicationCommitConcurrency, "number of concurrent replications")
	cmd.Flags().Bool("replication-allow-tx-discarding", options.ReplicationOptions.AllowTxDiscarding, "allow precommitted transactions to be discarded if the replica diverges from the primary")
	cmd.Flags().Bool("replication-skip-integrity-check", options.ReplicationOptions.SkipIntegrityCheck, "disable integrity check when reading data during replication")
	cmd.Flags().Bool("replication-wait-for-indexing", options.ReplicationOptions.WaitForIndexing, "wait for indexing to be up to date during replication")
	cmd.Flags().Int("max-active-databases", options.MaxActiveDatabases, "the maximum number of databases that can be active simultaneously")
	cmd.Flags().String("config", "", "config file (default path are configs or $HOME. Default filename is immudb.toml)")

	cmd.PersistentFlags().StringVar(&cl.config.CfgFn, "config", "", "config file (default path are configs or $HOME. Default filename is immudb.toml)")
	cmd.Flags().String("pidfile", options.Pidfile, "pid path with filename e.g. /var/run/immudb.pid")
	cmd.Flags().String("logdir", options.LogDir, "log path base dir /tmp/immudb/immulog")
	cmd.Flags().String("logfile", options.Logfile, "filename e.g. immudb.log")
	cmd.Flags().String("logformat", options.LogFormat, "log format e.g. text/json")
	cmd.Flags().Int("log-rotation-size", options.LogRotationSize, "maximum size a log segment can reach before being rotated")
	cmd.Flags().Duration("log-rotation-age", options.LogRotationAge, "maximum duration (age) of a log segment before it is rotated")
	cmd.Flags().Bool("log-access", options.LogAccess, "log incoming requests information (username, IP, etc...)")
	cmd.Flags().BoolP("mtls", "m", false, "enable mutual tls")
	cmd.Flags().BoolP("auth", "s", false, "enable auth")
	cmd.Flags().Int("max-recv-msg-size", options.MaxRecvMsgSize, "max message size in bytes the server can receive")
	cmd.Flags().Bool("no-histograms", false, "disable collection of histogram metrics like query durations")
	cmd.Flags().BoolP(c.DetachedFlag, c.DetachedShortFlag, options.Detached, "run immudb in background")
	cmd.Flags().Bool("auto-cert", options.AutoCert, "start the server using a generated, self-signed HTTPS certificate")
	cmd.Flags().String("certificate", "", "server certificate file path")
	cmd.Flags().String("pkey", "", "server private key path")
	cmd.Flags().String("clientcas", "", "clients certificates list. Aka certificate authority")
	cmd.Flags().Bool("devmode", options.DevMode, "enable dev mode: accept remote connections without auth")
	cmd.Flags().String("admin-password", options.AdminPassword, "admin password (default is 'immudb') as plain-text or base64 encoded (must be prefixed with 'enc:' if it is encoded)")
	cmd.Flags().Bool("force-admin-password", false, "if true, reset the admin password to the one passed through admin-password option upon startup")
	cmd.Flags().Bool("maintenance", options.GetMaintenance(), "override the authentication flag")
	cmd.Flags().String("signingKey", options.SigningKey, "signature private key path. If a valid one is provided, it enables the cryptographic signature of the root. e.g. \"./../test/signer/ec3.key\"")
	cmd.Flags().Bool("synced", true, "synced mode prevents data lost under unexpected crashes but affects performance")
	cmd.Flags().Int("token-expiry-time", options.TokenExpiryTimeMin, "client authentication token expiration time. Minutes")
	cmd.Flags().Bool("metrics-server", options.MetricsServer, "enable or disable Prometheus endpoint")
	cmd.Flags().Int("metrics-server-port", options.MetricsServerPort, "Prometheus endpoint port")
	cmd.Flags().Bool("web-server", options.WebServer, "enable or disable web/console server")
	cmd.Flags().Int("web-server-port", options.WebServerPort, "web/console server port")
	cmd.Flags().Bool("pgsql-server", true, "enable or disable pgsql server")
	cmd.Flags().Int("pgsql-server-port", 5432, "pgsql server port")
	cmd.Flags().Bool("pprof", false, "add pprof profiling endpoint on the metrics server")
	cmd.Flags().Bool("s3-storage", false, "enable or disable s3 storage")
	cmd.Flags().Bool("s3-role-enabled", false, "enable role-based authentication for s3 storage")
	cmd.Flags().String("s3-endpoint", "", "s3 endpoint")
	cmd.Flags().String("s3-role", "", "role name for role-based authentication attempt for s3 storage")
	cmd.Flags().String("s3-access-key-id", "", "s3 access key id")
	cmd.Flags().String("s3-secret-key", "", "s3 secret access key")
	cmd.Flags().String("s3-bucket-name", "", "s3 bucket name")
	cmd.Flags().String("s3-location", "", "s3 location (region)")
	cmd.Flags().String("s3-path-prefix", "", "s3 path prefix (multiple immudb instances can share the same bucket if they have different prefixes)")
	cmd.Flags().Bool("s3-external-identifier", false, "use the remote identifier if there is no local identifier")
	cmd.Flags().String("s3-instance-metadata-url", "http://169.254.169.254", "s3 instance metadata url")
	cmd.Flags().Int("max-sessions", 100, "maximum number of simultaneously opened sessions")
	cmd.Flags().Duration("max-session-inactivity-time", 3*time.Minute, "max session inactivity time is a duration after which an active session is declared inactive by the server. A session is kept active if server is still receiving requests from client (keep-alive or other methods)")
	cmd.Flags().Duration("max-session-age-time", 0, "the current default value is infinity. max session age time is a duration after which session will be forcibly closed")
	cmd.Flags().Duration("session-timeout", 2*time.Minute, "session timeout is a duration after which an inactive session is forcibly closed by the server")
	cmd.Flags().Duration("sessions-guard-check-interval", 1*time.Minute, "sessions guard check interval")
	cmd.Flags().MarkHidden("sessions-guard-check-interval")
	cmd.Flags().Bool("grpc-reflection", options.GRPCReflectionServerEnabled, "GRPC reflection server enabled")
	cmd.Flags().Bool("swaggerui", options.SwaggerUIEnabled, "Swagger UI enabled")
	cmd.Flags().Bool("log-request-metadata", options.LogRequestMetadata, "log request information in transaction metadata")

	flagNameMapping := map[string]string{
		"replication-enabled":           "replication-is-replica",
		"replication-follower-username": "replication-primary-username",
		"replication-follower-password": "replication-primary-password",
		"replication-master-database":   "replication-primary-database",
		"replication-master-address":    "replication-primary-host",
		"replication-master-port":       "replication-primary-port",
	}

	cmd.Flags().SetNormalizeFunc(func(f *pflag.FlagSet, name string) pflag.NormalizedName {
		if newName, ok := flagNameMapping[name]; ok {
			name = newName
		}
		return pflag.NormalizedName(name)
	})
}

func setupDefaults(options *server.Options) {
	viper.SetDefault("dir", options.Dir)
	viper.SetDefault("config", options.Config)
	viper.SetDefault("port", options.Port)
	viper.SetDefault("address", options.Address)
	viper.SetDefault("replica", false)
	viper.SetDefault("pidfile", options.Pidfile)
	viper.SetDefault("logfile", options.Logfile)
	viper.SetDefault("logdir", options.LogDir)
	viper.SetDefault("log-rotation-size", options.LogRotationSize)
	viper.SetDefault("log-rotation-age", options.LogRotationAge)
	viper.SetDefault("log-access", options.LogAccess)
	viper.SetDefault("mtls", false)
	viper.SetDefault("auth", options.GetAuth())
	viper.SetDefault("max-recv-msg-size", options.MaxRecvMsgSize)
	viper.SetDefault("no-histograms", options.NoHistograms)
	viper.SetDefault("detached", options.Detached)
	viper.SetDefault("auto-cert", options.AutoCert)
	viper.SetDefault("certificate", "")
	viper.SetDefault("pkey", "")
	viper.SetDefault("clientcas", "")
	viper.SetDefault("devmode", options.DevMode)
	viper.SetDefault("admin-password", options.AdminPassword)
	viper.SetDefault("force-admin-password", options.ForceAdminPassword)
	viper.SetDefault("maintenance", options.GetMaintenance())
	viper.SetDefault("synced", true)
	viper.SetDefault("token-expiry-time", options.TokenExpiryTimeMin)
	viper.SetDefault("metrics-server", options.MetricsServer)
	viper.SetDefault("metrics-server-port", options.MetricsServerPort)
	viper.SetDefault("web-server", options.WebServer)
	viper.SetDefault("web-server-port", options.WebServerPort)
	viper.SetDefault("pgsql-server", true)
	viper.SetDefault("pgsql-server-port", 5432)
	viper.SetDefault("pprof", false)
	viper.SetDefault("s3-storage", false)
	viper.SetDefault("s3-endpoint", "")
	viper.SetDefault("s3-role-enabled", false)
	viper.SetDefault("s3-role", "")
	viper.SetDefault("s3-access-key-id", "")
	viper.SetDefault("s3-secret-key", "")
	viper.SetDefault("s3-bucket-name", "")
	viper.SetDefault("s3-location", "")
	viper.SetDefault("s3-path-prefix", "")
	viper.SetDefault("s3-external-identifier", false)
	viper.SetDefault("s3-instance-metadata-url", "http://169.254.169.254")
	viper.SetDefault("max-sessions", 100)
	viper.SetDefault("max-session-inactivity-time", 3*time.Minute)
	viper.SetDefault("max-session-age-time", 0)
	viper.SetDefault("max-active-databases", options.MaxActiveDatabases)
	viper.SetDefault("session-timeout", 2*time.Minute)
	viper.SetDefault("sessions-guard-check-interval", 1*time.Minute)
	viper.SetDefault("logformat", logger.LogFormatText)
}

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

package server

import (
	"fmt"
	"net"
	"strconv"
	"strings"

	"github.com/codenotary/immudb/pkg/auth"
)

const SystemdbName = "systemdb"
const DefaultdbName = "defaultdb"

// Options server options list
type Options struct {
	Dir                 string
	Network             string
	Address             string
	Port                int
	MetricsPort         int
	Config              string
	Pidfile             string
	Logfile             string
	MTLs                bool
	MTLsOptions         MTLsOptions
	auth                bool
	NoHistograms        bool
	Detached            bool
	CorruptionCheck     bool
	MetricsServer       bool
	DevMode             bool
	AdminPassword       string `json:"-"`
	systemAdminDbName   string
	defaultDbName       string
	inMemoryStore       bool
	listener            net.Listener
	usingCustomListener bool
	maintenance         bool
	SignaturePrivateKey string
}

// DefaultOptions returns default server options
func DefaultOptions() Options {
	return Options{
		Dir:                 "./data",
		Network:             "tcp",
		Address:             "0.0.0.0",
		Port:                3322,
		MetricsPort:         9497,
		Config:              "configs/immudb.toml",
		Pidfile:             "",
		Logfile:             "",
		MTLs:                false,
		auth:                true,
		NoHistograms:        false,
		Detached:            false,
		CorruptionCheck:     true,
		MetricsServer:       true,
		DevMode:             false,
		AdminPassword:       auth.SysAdminPassword,
		systemAdminDbName:   SystemdbName,
		defaultDbName:       DefaultdbName,
		inMemoryStore:       false,
		usingCustomListener: false,
		maintenance:         false,
	}
}

// WithDir sets dir
func (o Options) WithDir(dir string) Options {
	o.Dir = dir
	return o
}

// WithNetwork sets network
func (o Options) WithNetwork(network string) Options {
	o.Network = network
	return o
}

// WithAddress sets address
func (o Options) WithAddress(address string) Options {
	o.Address = address
	return o
}

// WithPort sets port
func (o Options) WithPort(port int) Options {
	if port > 0 {
		o.Port = port
	}
	return o
}

// WithConfig sets config file name
func (o Options) WithConfig(config string) Options {
	o.Config = config
	return o
}

// WithPidfile sets pid file
func (o Options) WithPidfile(pidfile string) Options {
	o.Pidfile = pidfile
	return o
}

// WithLogfile sets logfile
func (o Options) WithLogfile(logfile string) Options {
	o.Logfile = logfile
	return o
}

// WithMTLs sets mtls
func (o Options) WithMTLs(MTLs bool) Options {
	o.MTLs = MTLs
	return o
}

// WithMTLsOptions sets WithMTLsOptions
func (o Options) WithMTLsOptions(MTLsOptions MTLsOptions) Options {
	o.MTLsOptions = MTLsOptions
	return o
}

// WithAuth sets auth
func (o Options) WithAuth(authEnabled bool) Options {
	o.auth = authEnabled
	return o
}

// GetAuth gets auth
func (o Options) GetAuth() bool {
	if o.maintenance {
		return false
	}
	return o.auth
}

// WithNoHistograms disables collection of histograms metrics (e.g. query durations)
func (o Options) WithNoHistograms(noHistograms bool) Options {
	o.NoHistograms = noHistograms
	return o
}

// WithDetached sets immudb to be run in background
func (o Options) WithDetached(detached bool) Options {
	o.Detached = detached
	return o
}

// WithCorruptionCheck enable corruption check
func (o Options) WithCorruptionCheck(corruptionCheck bool) Options {
	o.CorruptionCheck = corruptionCheck
	return o
}

// Bind returns bind address
func (o Options) Bind() string {
	return o.Address + ":" + strconv.Itoa(o.Port)
}

// MetricsBind return metrics bind address
func (o Options) MetricsBind() string {
	return o.Address + ":" + strconv.Itoa(o.MetricsPort)
}

// String print options
func (o Options) String() string {
	rightPad := func(k string, v interface{}) string {
		return fmt.Sprintf("%-16s: %v", k, v)
	}
	opts := make([]string, 0, 16)
	opts = append(opts, "================ Config ================")
	opts = append(opts, rightPad("Data dir", o.Dir))
	opts = append(opts, rightPad("Address", fmt.Sprintf("%s:%d", o.Address, o.Port)))
	if o.MetricsServer {
		opts = append(opts, rightPad("Metrics address", fmt.Sprintf("%s:%d/metrics", o.Address, o.MetricsPort)))
	}
	if o.Config != "" {
		opts = append(opts, rightPad("Config file", o.Config))
	}
	if o.Pidfile != "" {
		opts = append(opts, rightPad("PID file", o.Pidfile))
	}
	if o.Logfile != "" {
		opts = append(opts, rightPad("Log file", o.Logfile))
	}
	opts = append(opts, rightPad("MTLS enabled", o.MTLs))
	opts = append(opts, rightPad("Auth enabled", o.auth))
	opts = append(opts, rightPad("Dev mode", o.DevMode))
	opts = append(opts, rightPad("Default database", o.defaultDbName))
	opts = append(opts, rightPad("Maintenance mode", o.maintenance))
	opts = append(opts, "----------------------------------------")
	opts = append(opts, "Superadmin default credentials")
	opts = append(opts, rightPad("   Username", auth.SysAdminUsername))
	opts = append(opts, rightPad("   Password", auth.SysAdminPassword))
	opts = append(opts, "========================================")
	return strings.Join(opts, "\n")
}

// WithMetricsServer ...
func (o Options) WithMetricsServer(metricsServer bool) Options {
	o.MetricsServer = metricsServer
	return o
}

// WithDevMode ...
func (o Options) WithDevMode(devMode bool) Options {
	o.DevMode = devMode
	return o
}

// WithAdminPassword ...
func (o Options) WithAdminPassword(adminPassword string) Options {
	o.AdminPassword = adminPassword
	return o
}

//GetSystemAdminDbName returns the System database name
func (o Options) GetSystemAdminDbName() string {
	return o.systemAdminDbName
}

//GetDefaultDbName returns the default database name
func (o Options) GetDefaultDbName() string {
	return o.defaultDbName
}

// WithInMemoryStore use in memory database without persistence, used for tests
func (o Options) WithInMemoryStore(inmemory bool) Options {
	o.inMemoryStore = inmemory
	return o
}

//GetInMemoryStore returns if we use in memory database without persistence , used for tests
func (o Options) GetInMemoryStore() bool {
	return o.inMemoryStore
}

// WithListener used usually to pass a bufered listener for testing purposes
func (o Options) WithListener(lis net.Listener) Options {
	o.listener = lis
	o.usingCustomListener = true
	return o
}

// WithMaintenance sets maintenance mode
func (o Options) WithMaintenance(m bool) Options {
	o.maintenance = m
	return o
}

// GetMaintenance gets maintenance mode
func (o Options) GetMaintenance() bool {
	return o.maintenance
}

// WithSignaturePrivateKey sets signature private key
func (o Options) WithSignaturePrivateKey(signaturePrivateKey string) Options {
	o.SignaturePrivateKey = signaturePrivateKey
	return o
}

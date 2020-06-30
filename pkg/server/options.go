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
	"encoding/json"
	"net"
	"strconv"

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
}

// DefaultOptions returns default server options
func DefaultOptions() Options {
	return Options{
		Dir:                 "./data",
		Network:             "tcp",
		Address:             "127.0.0.1",
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
		DevMode:             true,
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
	o.Port = port
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
	optionsJSON, err := json.Marshal(o)
	if err != nil {
		return err.Error()
	}
	return string(optionsJSON)
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

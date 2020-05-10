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
	"strconv"
)

// Options server options list
type Options struct {
	Dir          string
	Network      string
	Address      string
	Port         int
	MetricsPort  int
	DbName       string
	SysDbName    string
	Config       string
	Pidfile      string
	Logfile      string
	MTLs         bool
	MTLsOptions  MTLsOptions
	Auth         bool
	NoHistograms bool
	Detached     bool
}

// DefaultOptions returns default server options
func DefaultOptions() Options {
	return Options{
		Dir:          "./db",
		Network:      "tcp",
		Address:      "127.0.0.1",
		Port:         3322,
		MetricsPort:  9497,
		DbName:       "immudb",
		SysDbName:    "immudbsys",
		Config:       "configs/immudb.toml",
		Pidfile:      "",
		Logfile:      "",
		MTLs:         false,
		Auth:         false,
		NoHistograms: false,
		Detached:     false,
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

// WithDbName sets dbName
func (o Options) WithDbName(dbName string) Options {
	o.DbName = dbName
	return o
}

// WithSysDbName sets SysDbName
func (o Options) WithSysDbName(sysDbName string) Options {
	o.SysDbName = sysDbName
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
	o.Auth = authEnabled
	return o
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
	optionsJson, err := json.Marshal(o)
	if err != nil {
		return err.Error()
	}
	return string(optionsJson)
}

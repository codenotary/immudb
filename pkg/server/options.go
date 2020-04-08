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
	"strconv"
)

// Options server options list
type Options struct {
	Dir         string
	Network     string
	Address     string
	Port        int
	MetricsPort int
	DbName      string
	Config      string
	Pidfile     string
	Logfile     string
	MTLs        bool
	MTLsOptions MTLsOptions
}

// DefaultOptions returns default server options
func DefaultOptions() Options {
	return Options{
		Dir:         ".",
		Network:     "tcp",
		Address:     "127.0.0.1",
		Port:        3322,
		MetricsPort: 9497,
		DbName:      "immudb",
		Config:      "configs/immucfg.ini",
		Pidfile:     "",
		Logfile:     "",
		MTLs:        false,
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
	return fmt.Sprintf(
		"{dir:%v network:%v address:%v port:%d metrics:%d name:%v config file:%v pid:%v log:%v MTLs:%v}",
		o.Dir, o.Network, o.Address, o.Port, o.MetricsPort, o.DbName, o.Config, o.Pidfile, o.Logfile, o.MTLs)
}

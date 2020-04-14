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

package gw

import (
	"fmt"
	"strconv"

	"github.com/codenotary/immudb/pkg/client"
)

type Options struct {
	Address       string
	Port          int
	ImmudbAddress string
	ImmudbPort    int
	MTLs          bool
	MTLsOptions   client.MTLsOptions
	Config        string
	Pidfile       string
	Logfile       string
}

func DefaultOptions() Options {
	return Options{
		Address:       "127.0.0.1",
		Port:          3323,
		ImmudbAddress: "127.0.0.1",
		ImmudbPort:    3322,
		MTLs:          false,
		Config:        "configs/immugw.ini",
		Pidfile:       "",
		Logfile:       "",
	}
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

// WithImmudbAddress sets immudbAddress
func (o Options) WithImmudbAddress(immudbAddress string) Options {
	o.ImmudbAddress = immudbAddress
	return o
}

// WithImmudbPort sets immudbPort
func (o Options) WithImmudbPort(immudbPort int) Options {
	o.ImmudbPort = immudbPort
	return o
}

// WithMTLs sets MTLs
func (o Options) WithMTLs(MTLs bool) Options {
	o.MTLs = MTLs
	return o
}

// WithMTLsOptions sets MTLsOptions
func (o Options) WithMTLsOptions(MTLsOptions client.MTLsOptions) Options {
	o.MTLsOptions = MTLsOptions
	return o
}

// WithConfig sets config
func (o Options) WithConfig(config string) Options {
	o.Config = config
	return o
}

// WithPidfile sets pidfile
func (o Options) WithPidfile(pidfile string) Options {
	o.Pidfile = pidfile
	return o
}

// WithLogfile sets logfile
func (o Options) WithLogfile(logfile string) Options {
	o.Logfile = logfile
	return o
}

func (o Options) Bind() string {
	return o.Address + ":" + strconv.Itoa(o.Port)
}

func (o Options) String() string {
	return fmt.Sprintf(
		"{address:%v port:%d immudb-address:%v immudb-port:%d config file:%v pid:%v log:%v MTLs:%v}",
		o.Address, o.Port, o.ImmudbAddress, o.ImmudbPort, o.Config, o.Pidfile, o.Logfile, o.MTLs)
}

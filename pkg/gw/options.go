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
	"github.com/codenotary/immudb/pkg/client"
	"strconv"
)

type Options struct {
	Address      string
	Port         int
	ImmudAddress string
	ImmudPort    int
	MTLs         bool
	MTLsOptions  client.MTLsOptions
	Config       string
	Pidfile      string
	Logfile      string
}

func DefaultOptions() Options {
	return Options{
		Address:      "127.0.0.1",
		Port:         3323,
		ImmudAddress: "127.0.0.1",
		ImmudPort:    3322,
		MTLs:         false,
		Config:       "configs/immu.toml",
		Pidfile:      "",
		Logfile:      "",
	}
}

func (o Options) WithAddress(address string) Options {
	o.Address = address
	return o
}

func (o Options) WithPort(port int) Options {
	o.Port = port
	return o
}

func (o Options) WithImmudAddress(immudAddress string) Options {
	o.ImmudAddress = immudAddress
	return o
}

func (o Options) WithImmudPort(immudPort int) Options {
	o.ImmudPort = immudPort
	return o
}

func (o Options) WithMTLs(MTLs bool) Options {
	o.MTLs = MTLs
	return o
}

func (o Options) WithMTLsOptions(MTLsOptions client.MTLsOptions) Options {
	o.MTLsOptions = MTLsOptions
	return o
}

func (o Options) WithConfig(config string) Options {
	o.Config = config
	return o
}

func (o Options) WithPidfile(pidfile string) Options {
	o.Pidfile = pidfile
	return o
}

func (o Options) WithLogfile(logfile string) Options {
	o.Logfile = logfile
	return o
}

func (o Options) Bind() string {
	return o.Address + ":" + strconv.Itoa(o.Port)
}

func (o Options) String() string {
	return fmt.Sprintf(
		"{address:%v port:%d immud-address:%v immud-port:%d config file:%v pid:%v log:%v MTLs:%v}",
		o.Address, o.Port, o.ImmudAddress, o.ImmudPort, o.Config, o.Pidfile, o.Logfile, o.MTLs)
}

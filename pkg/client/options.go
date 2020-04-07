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

package client

import (
	"fmt"
	"strconv"

	"google.golang.org/grpc"
)

type Options struct {
	Address            string
	Port               int
	DialRetries        int
	HealthCheckRetries int
	MTLs               bool
	MTLsOptions        MTLsOptions
	DialOptions        []grpc.DialOption
	Config             string
	Pidfile            string
	Logfile            string
}

func DefaultOptions() Options {
	return Options{
		Address:            "127.0.0.1",
		Port:               3322,
		DialRetries:        5,
		HealthCheckRetries: 5,
		MTLs:               false,
		Config:             "configs/immu.toml",
		Pidfile:            "",
		Logfile:            "",
		DialOptions:        []grpc.DialOption{},
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

// WithDialRetries sets dial retries
func (o Options) WithDialRetries(retries int) Options {
	o.DialRetries = retries
	return o
}

// WithHealthCheckRetries sets healt check retries
func (o Options) WithHealthCheckRetries(retries int) Options {
	o.HealthCheckRetries = retries
	return o
}

// WithMTLs activate/deactivate MTLs
func (o Options) WithMTLs(MTLs bool) Options {
	o.MTLs = MTLs
	return o
}

// WithConfig sets config file name
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

// WithMTLsOptions sets MTLsOptions
func (o Options) WithMTLsOptions(MTLsOptions MTLsOptions) Options {
	o.MTLsOptions = MTLsOptions
	return o
}

// WithDialOptions sets dialOptions
func (o Options) WithDialOptions(replaceExisting bool, dialOptions ...grpc.DialOption) Options {
	if replaceExisting {
		o.DialOptions = dialOptions
	} else {
		o.DialOptions = append(o.DialOptions, dialOptions...)
	}
	return o
}

func (o Options) Bind() string {
	return o.Address + ":" + strconv.Itoa(o.Port)
}

func (o Options) String() string {
	return fmt.Sprintf("{address:%v port:%d}", o.Address, o.Port)
}

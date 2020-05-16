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
	"encoding/json"
	"strconv"

	"google.golang.org/grpc"
)

type Options struct {
	Dir                string
	Address            string
	Port               int
	HealthCheckRetries int
	MTLs               bool
	MTLsOptions        MTLsOptions
	Auth               bool
	DialOptions        *[]grpc.DialOption
	Config             string
	TokenFileName      string
}

func DefaultOptions() *Options {
	return &Options{
		Dir:                ".",
		Address:            "127.0.0.1",
		Port:               3322,
		HealthCheckRetries: 5,
		MTLs:               false,
		Auth:               false,
		Config:             "configs/immuclient.toml",
		TokenFileName:      "token",
		DialOptions:        &[]grpc.DialOption{},
	}
}

// WithDir sets program file folder
func (o Options) WithDir(dir string) Options {
	o.Dir = dir
	return o
}

// WithAddress sets address
func (o *Options) WithAddress(address string) *Options {
	o.Address = address
	return o
}

// WithPort sets port
func (o *Options) WithPort(port int) *Options {
	o.Port = port
	return o
}

// WithHealthCheckRetries sets healt check retries
func (o *Options) WithHealthCheckRetries(retries int) *Options {
	o.HealthCheckRetries = retries
	return o
}

// WithMTLs activate/deactivate MTLs
func (o *Options) WithMTLs(MTLs bool) *Options {
	o.MTLs = MTLs
	return o
}

// WithAuth activate/deactivate auth
func (o *Options) WithAuth(authEnabled bool) *Options {
	o.Auth = authEnabled
	return o
}

// WithConfig sets config file name
func (o *Options) WithConfig(config string) *Options {
	o.Config = config
	return o
}

// WithTokenFileName sets token file name
func (o *Options) WithTokenFileName(tokenFileName string) *Options {
	o.TokenFileName = tokenFileName
	return o
}

// WithMTLsOptions sets MTLsOptions
func (o *Options) WithMTLsOptions(MTLsOptions MTLsOptions) *Options {
	o.MTLsOptions = MTLsOptions
	return o
}

// WithDialOptions sets dialOptions
func (o *Options) WithDialOptions(dialOptions *[]grpc.DialOption) *Options {
	o.DialOptions = dialOptions
	return o
}

func (o *Options) Bind() string {
	return o.Address + ":" + strconv.Itoa(o.Port)
}

func (o *Options) String() string {
	optionsJson, err := json.Marshal(o)
	if err != nil {
		return err.Error()
	}
	return string(optionsJson)
}

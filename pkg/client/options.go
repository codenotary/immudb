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

	c "github.com/codenotary/immudb/cmd/helper"
	"google.golang.org/grpc"
)

// AdminTokenFileSuffix is the suffix used for the token file name
const AdminTokenFileSuffix = "_admin"

// Options client options
type Options struct {
	Dir                string
	Address            string
	Port               int
	HealthCheckRetries int
	MTLs               bool
	MTLsOptions        MTLsOptions
	Auth               bool
	MaxRecvMsgSize     int
	DialOptions        *[]grpc.DialOption
	Config             string
	TokenFileName      string
	CurrentDatabase    string
	PasswordReader     c.PasswordReader
	Tkns               TokenService
	Metrics            bool
	PidPath            string
	PrometheusHost     string
	PrometheusPort     string
	LogFileName        string
}

// DefaultOptions ...
func DefaultOptions() *Options {
	return &Options{
		Dir:                ".",
		Address:            "127.0.0.1",
		Port:               3322,
		HealthCheckRetries: 5,
		MTLs:               false,
		Auth:               true,
		MaxRecvMsgSize:     4 * 1024 * 1024, //4Mb
		Config:             "configs/immuclient.toml",
		TokenFileName:      "token",
		DialOptions:        &[]grpc.DialOption{},
		PasswordReader:     c.DefaultPasswordReader,
		Tkns:               NewTokenService().WithTokenFileName("token").WithHds(NewHomedirService()),
		Metrics:            true,
		PidPath:            "",
		PrometheusHost:     "",
		PrometheusPort:     "",
		LogFileName:        "",
	}
}

// WithLogFileName set log file name
func (o *Options) WithLogFileName(filename string) *Options {
	o.LogFileName = filename
	return o
}

// WithPrometheusHost set prometheus host
func (o *Options) WithPrometheusHost(host string) *Options {
	o.PrometheusHost = host
	return o
}

// WithPrometheusPort set prometheus port
func (o *Options) WithPrometheusPort(port string) *Options {
	o.PrometheusPort = port
	return o
}

// WithPidPath set pid file path
func (o *Options) WithPidPath(path string) *Options {
	o.PidPath = path
	return o
}

// WithMetrics set if metrics should start
func (o *Options) WithMetrics(start bool) *Options {
	o.Metrics = start
	return o
}

// WithDir sets program file folder
func (o *Options) WithDir(dir string) *Options {
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
	if port > 0 {
		o.Port = port
	}
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

// MaxRecvMsgSize max recv msg size in bytes
func (o *Options) WithMaxRecvMsgSize(maxRecvMsgSize int) *Options {
	o.MaxRecvMsgSize = maxRecvMsgSize
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

// Bind concatenates address and port
func (o *Options) Bind() string {
	return o.Address + ":" + strconv.Itoa(o.Port)
}

// WithPasswordReader sets the password reader for the client
func (o *Options) WithPasswordReader(pr c.PasswordReader) *Options {
	o.PasswordReader = pr
	return o
}

// WithTokenService sets the TokenService for the client
func (o *Options) WithTokenService(tkns TokenService) *Options {
	o.Tkns = tkns
	return o
}

func (o *Options) String() string {
	optionsJSON, err := json.Marshal(o)
	if err != nil {
		return err.Error()
	}
	return string(optionsJSON)
}

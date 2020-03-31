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
	"os"
	"strconv"
)

type Options struct {
	Address      string
	Port         int
	ImmudAddress string
	ImmudPort    int
	MTLs         bool
	MTLsOptions  client.MTLsOptions
}

func DefaultOptions() Options {
	return Options{
		Address:      "127.0.0.1",
		Port:         8081,
		ImmudAddress: "127.0.0.1",
		ImmudPort:    8080,
		MTLs:         false,
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

func (o Options) Bind() string {
	return o.Address + ":" + strconv.Itoa(o.Port)
}

func (o Options) String() string {
	return fmt.Sprintf(
		"{address:%v port:%d immud-address:%v immud-port:%d MTLs:%v}",
		o.Address, o.Port, o.ImmudAddress, o.ImmudPort, o.MTLs)
}

func (o Options) FromEnvironment() Options {
	address := os.Getenv("IMMUGW_ADDRESS")
	if address != "" {
		o.Address = address
	}
	port := os.Getenv("IMMUGW_PORT")
	if parsedPort, err := strconv.Atoi(port); err == nil {
		o.Port = parsedPort
	}
	immudAddress := os.Getenv("IMMU_ADDRESS")
	if immudAddress != "" {
		o.ImmudAddress = immudAddress
	}
	immudPort := os.Getenv("IMMU_PORT")
	if parsedPort, err := strconv.Atoi(immudPort); err == nil {
		o.ImmudPort = parsedPort
	}
	if MTLs, err := strconv.ParseBool(os.Getenv("IMMU_MTLS")); err == nil {
		o.MTLs = MTLs
	}
	if o.MTLs {
		mo := client.MTLsOptions{}
		o.MTLsOptions = mo.FromEnvironment()
	}
	return o
}

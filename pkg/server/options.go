/*
Copyright 2019 vChain, Inc.

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
	"os"
	"strconv"
)

type Options struct {
	Dir     string
	Network string
	Address string
	Port    int
	DbName  string
}

func DefaultOptions() Options {
	return Options{
		Dir:     ".",
		Network: "tcp",
		Address: "127.0.0.1",
		Port:    8080,
		DbName:  "immudb",
	}
}

func (o Options) WithDir(dir string) Options {
	o.Dir = dir
	return o
}

func (o Options) WithNetwork(network string) Options {
	o.Network = network
	return o
}

func (o Options) WithAddress(address string) Options {
	o.Address = address
	return o
}

func (o Options) WithPort(port int) Options {
	o.Port = port
	return o
}

func (o Options) WithDbName(dbName string) Options {
	o.DbName = dbName
	return o
}

func (o Options) Bind() string {
	return o.Address + ":" + strconv.Itoa(o.Port)
}

func (o Options) String() string {
	return fmt.Sprintf(
		"{dir:%v network:%v address:%v port:%d name:%v}",
		o.Dir, o.Network, o.Address, o.Port, o.DbName)
}

func (o Options) FromEnvironment() Options {
	dir := os.Getenv("IMMUDB_DIR")
	if dir != "" {
		o.Dir = dir
	}
	network := os.Getenv("IMMUDB_NETWORK")
	if network != "" {
		o.Network = network
	}
	address := os.Getenv("IMMUDB_ADDRESS")
	if address != "" {
		o.Address = address
	}
	port := os.Getenv("IMMUDB_PORT")
	if parsedPort, err := strconv.Atoi(port); err == nil {
		o.Port = parsedPort
	}
	dbName := os.Getenv("IMMUDB_DBNAME")
	if dbName != "" {
		o.DbName = dbName
	}
	return o
}

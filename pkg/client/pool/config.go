/*
Copyright 2022 Codenotary Inc. All rights reserved.

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

package pool

import (
	"time"

	"github.com/codenotary/immudb/pkg/client"
)

// Config contains setting necessary to initialise a client pool
type Config struct {
	Db             string // database name to operate on
	Address        string // host address
	Port           int    // immudb server port
	Options        *client.Options
	Heartbeat      time.Duration // the duration between checks of the health of idle connections
	MaxConnections int           // no of maximum connections allowed per client
}

func (c *Config) WithDb(db string) *Config {
	c.Options.WithDatabase(db)
	c.Db = db
	return c
}

func (c *Config) WithAddress(addr string) *Config {
	c.Options.WithAddress(addr)
	c.Address = addr
	return c
}

func (c *Config) WithPort(port int) *Config {
	c.Options.WithPort(port)
	c.Port = port
	return c
}

func (c *Config) WithOptions(opt *client.Options) *Config {
	c.Options = opt
	return c
}

func (c *Config) WithHeatbeat(t time.Duration) *Config {
	c.Heartbeat = t
	return c
}

func (c *Config) WithMaxConnections(m int) *Config {
	c.MaxConnections = m
	return c
}

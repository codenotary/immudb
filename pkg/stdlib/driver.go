/*
Copyright 2021 CodeNotary, Inc. All rights reserved.

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

package stdlib

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"github.com/codenotary/immudb/pkg/client"
	"sync"
	"time"
)

var immuDriver *Driver

func init() {
	immuDriver = &Driver{
		configs: make(map[string]*Conn),
	}
	sql.Register("immudb", immuDriver)
}

func OpenDB(cliOpts *client.Options) *sql.DB {
	c := &immuConnector{
		cliOptions: cliOpts,
		driver:     immuDriver,
	}
	return sql.OpenDB(c)
}

type Driver struct {
	configMutex sync.Mutex
	configs     map[string]*Conn
	seq         int
}

// Open
func (d *Driver) Open(name string) (driver.Conn, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	connector, _ := d.OpenConnector(name)
	return connector.Connect(ctx)
}

func (d *Driver) OpenConnector(name string) (driver.Connector, error) {
	return &driverConnector{driver: d, name: name}, nil
}

func (d *Driver) UnregisterConnection(name string) {
	d.configMutex.Lock()
	defer d.configMutex.Unlock()
	delete(d.configs, name)
}

func (d *Driver) RegisterConnection(cn *Conn) string {
	d.configMutex.Lock()
	defer d.configMutex.Unlock()
	name := fmt.Sprintf("registeredConnConfig%d", d.seq)
	d.seq++
	d.configs[name] = cn
	return name
}

func (d *Driver) GetNewConnByOptions(ctx context.Context, cliOptions *client.Options) (*Conn, error) {
	conn := client.NewClient().WithOptions(cliOptions)

	name := GetUri(cliOptions)

	err := conn.OpenSession(ctx, []byte(cliOptions.Username), []byte(cliOptions.Password), cliOptions.Database)
	if err != nil {
		return nil, err
	}

	cn := &Conn{
		name:    name,
		conn:    conn,
		options: cliOptions,
		driver:  d,
	}
	return cn, nil
}

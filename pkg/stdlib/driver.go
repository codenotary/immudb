/*
Copyright 2024 Codenotary Inc. All rights reserved.

SPDX-License-Identifier: BUSL-1.1
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    https://mariadb.com/bsl11/

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
	"sync"
	"time"

	"github.com/codenotary/immudb/pkg/client"
)

var immuDriver *Driver

func init() {

	immuDriver = &Driver{
		clientOptions: make(map[string]*client.Options),
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

func Open(dns string) *sql.DB {
	c := &driverConnector{
		driver: immuDriver,
		name:   dns,
	}
	return sql.OpenDB(c)
}

type Driver struct {
	configMutex   sync.Mutex
	clientOptions map[string]*client.Options
	sequence      int
}

func (d *Driver) Open(name string) (driver.Conn, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second) // Ensure eventual timeout
	defer cancel()

	connector, err := d.OpenConnector(name)
	if err != nil {
		return nil, err
	}
	return connector.Connect(ctx)
}

func (d *Driver) OpenConnector(name string) (driver.Connector, error) {
	return &driverConnector{driver: d, name: name}, nil
}

func (d *Driver) registerConnConfig(opt *client.Options) string {
	d.configMutex.Lock()
	connStr := fmt.Sprintf("registeredConnConfig%d", d.sequence)
	d.sequence++
	d.clientOptions[connStr] = opt
	d.configMutex.Unlock()
	return connStr
}

func (d *Driver) unregisterConnConfig(connStr string) {
	d.configMutex.Lock()
	delete(d.clientOptions, connStr)
	d.configMutex.Unlock()
}

// RegisterConnConfig registers a ConnConfig and returns the connection string to use with Open.
func RegisterConnConfig(clientOptions *client.Options) string {
	return immuDriver.registerConnConfig(clientOptions)
}

// UnregisterConnConfig removes the ConnConfig registration for connStr.
func UnregisterConnConfig(connStr string) {
	immuDriver.unregisterConnConfig(connStr)
}
func (d *Driver) getNewConnByOptions(ctx context.Context, cliOptions *client.Options) (*Conn, error) {
	immuClient := client.NewClient().WithOptions(cliOptions)

	name := GetUri(cliOptions)

	err := immuClient.OpenSession(ctx, []byte(cliOptions.Username), []byte(cliOptions.Password), cliOptions.Database)
	if err != nil {
		return nil, err
	}

	cn := &Conn{
		name:       name,
		immuClient: immuClient,
		options:    cliOptions,
		driver:     d,
	}

	return cn, nil
}

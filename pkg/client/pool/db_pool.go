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

// DatabasePool maintains a connection pool for multiple databases in an immudb instance.
type DatabasePool struct {
	connections map[string]*ClientPool
}

// NewDatabasePool creates and returns a new DatabasePool object.
//
// Heartbeat timeout is the timeout for checking the health of a
// connection to the immudb instance.
//
// Max connections per database is the size of the connection pool
// for each database client connected to a single immudb instance.
func NewDatabasePool(
	address string,
	port int,
	dbs []string,
	heartbeat time.Duration,
	maxConnectionsPerDatabase int,
	opt *client.Options,
) *DatabasePool {
	connections := make(map[string]*ClientPool, len(dbs))
	for _, db := range dbs {
		conf := &Config{
			Db:             db,
			Address:        address,
			Port:           port,
			Heartbeat:      heartbeat,
			Options:        opt,
			MaxConnections: maxConnectionsPerDatabase,
		}
		connections[db] = NewClientPool(conf)
	}
	return &DatabasePool{
		connections: connections,
	}
}

// With returns a reference to the client pool for the given database.
func (p *DatabasePool) With(db string) *ClientPool {
	return p.connections[db]
}

// DoWith is a convenience function which combines With and Do functions
// to execute the given function on the specified database.
func (p *DatabasePool) DoWith(db string, do func(client.ImmuClient) error) error {
	return p.With(db).Do(do)
}

// Close closes all database connections in the cluster.
func (p *DatabasePool) Close() error {
	var lastErr error
	for _, cli := range p.connections {
		err := cli.Close()
		if err != nil {
			lastErr = err
		}
	}
	return lastErr
}

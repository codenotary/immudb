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
	"context"
	"sync"
	"time"

	"github.com/codenotary/immudb/pkg/client"
)

// ClientPool maintains a client connection pool for multiple Immudb instances for a particular database.
type ClientPool struct {
	mu          *sync.Mutex
	cond        *sync.Cond
	conf        *Config                           // options to connect to DB
	connections []client.ImmuClient               // active connection pool
	init        func() (client.ImmuClient, error) // factory to add dependendy injection to create immuclient, useful for testing
	total       int                               // no of outstanding connections
	max         int                               // no of maximum connections allowed
	stopOnce    sync.Once
	stopChan    chan struct{}
}

// NewClientPool creates a new connection pool to an immudb server
// for a particular database.
//
// Config is used to connect to a database on an immudb server.
func NewClientPool(cfg *Config) *ClientPool {
	mu := &sync.Mutex{}
	cond := sync.NewCond(mu)

	// setup options for immudb server
	if cfg.Options == nil {
		cfg.WithOptions(client.DefaultOptions())
	}
	cfg.Options = cfg.Options.WithAddress(cfg.Address).WithPort(cfg.Port).WithDatabase(cfg.Db)

	// create the client pool instance
	p := &ClientPool{
		mu:          mu,
		cond:        cond,
		conf:        cfg,
		total:       0,
		max:         cfg.MaxConnections,
		stopChan:    make(chan struct{}),
		connections: make([]client.ImmuClient, 0),
	}

	// setup the factory for immudb client creation
	p.init = func() (client.ImmuClient, error) {
		ctx := context.Background()
		// create a new client connection with a new session
		cli := client.NewClient().WithOptions(p.conf.Options)
		err := cli.OpenSession(ctx, []byte(p.conf.Options.Username), []byte(p.conf.Options.Password), p.conf.Options.Database)
		if err != nil {
			return nil, err
		}
		return cli, nil
	}

	// start health check runner in the background
	if cfg.Heartbeat > 0 {
		go p.runHealthCheck()
	}

	return p
}

// Get returns a new connection to an immudb instance
func (p *ClientPool) Get() (client.ImmuClient, error) {
	p.mu.Lock()
	for {
		available := len(p.connections)
		switch {
		// Connections exhausted, wait for a connection from the pool
		case available == 0 && p.total >= p.max:
			// This is a blocking call which waits for a signal to continue.
			// If there are no available clients in the pool, and max connections
			// have been reached, the goroutine is blocked until a connection is
			// returned to the pool (using the Put() method). Once returned, this
			// will unblock and in the next loop the below cases will be executed.
			p.cond.Wait()

		// Connection unavailable, create a new connection and return
		case available == 0 && p.total < p.max:
			cli, err := p.init()
			if err == nil {
				p.total++
			}
			p.mu.Unlock()
			return cli, err

		// Connection available, return one from the pool
		case available > 0:
			var conn client.ImmuClient
			conn, p.connections = p.connections[0], p.connections[1:]
			if p.total < p.max {
				p.total++
			}
			p.mu.Unlock()
			return conn, nil
		}
	}
}

// Put returns a connection back to the connection pool
func (p *ClientPool) Put(conn client.ImmuClient) {
	p.mu.Lock()
	defer p.mu.Unlock()

	// if the maximum connection limit is reached, close
	// the connection and return
	if len(p.connections) >= p.max {
		conn.CloseSession(context.Background())
		conn.Disconnect()
		return
	}

	p.connections = append(p.connections, conn)
	if p.total > 0 {
		p.total--
	}
	p.cond.Signal()
}

// Do gets a connection from the pool and calls the given function. After the
// function is executed, it returns the connection back to the pool.
func (p *ClientPool) Do(do func(client.ImmuClient) error) error {
	conn, err := p.Get()
	if err != nil {
		return err
	}
	defer p.Put(conn)
	return do(conn)
}

// runHealthCheck runs a periodic check for faulty connections in the pool
func (p *ClientPool) runHealthCheck() {
	ticker := time.NewTicker(p.conf.Heartbeat)
	for {
		select {
		case <-p.stopChan:
			ticker.Stop()
			return
		case <-ticker.C:
			p.healthCheck()
		}
	}
}

// healthCheck checks for faulty connections in the connection pool and closes it.
// This is useful when a connection from the pool encounters an error connecting
// to the db, or the session has expired. healthCheck will free the connection slot
// for the pool, and a new connection can then be requested.
func (p *ClientPool) healthCheck() {
	p.mu.Lock()
	defer p.mu.Unlock()

	for i := len(p.connections) - 1; i >= 0; i-- {
		conn := p.connections[i]
		_, err := conn.Health(context.Background())
		// if the connection is broken, remove it from the pool
		if err != nil {
			p.connections = append(p.connections[:i], p.connections[i+1:]...)
			conn.CloseSession(context.Background())
			conn.Disconnect()
		}
	}
}

// Close closes all connections in the pool, and if there is an error closing
// the connection, it returns the last error found.
func (p *ClientPool) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	var err error
	p.stopOnce.Do(func() {
		close(p.stopChan)
		err = p.close()
	})
	return err
}

func (p *ClientPool) close() error {
	var lastErr error
	for _, conn := range p.connections {
		err := conn.CloseSession(context.Background())
		if err != nil {
			lastErr = err
		}
		err = conn.Disconnect()
		if err != nil {
			lastErr = err
		}
	}
	p.connections = p.connections[:0]
	return lastErr
}

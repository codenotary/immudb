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
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/codenotary/immudb/pkg/client"
	gomock "github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
)

var (
	errTimedOut = errors.New("timed out")
	newTestPool = func(size int, timeout time.Duration, t *testing.T) *ClientPool {
		cnf := &Config{MaxConnections: size, Heartbeat: timeout}
		p := NewClientPool(cnf)
		p.init = func() (client.ImmuClient, error) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()
			m := client.NewMockImmuClient(ctrl)
			m.EXPECT().Health(context.TODO()).Return(nil, nil).AnyTimes()
			m.EXPECT().CloseSession(context.Background()).Return(nil).AnyTimes()
			m.EXPECT().Disconnect().Return(nil).AnyTimes()
			return m, nil
		}
		return p
	}
)

func TestClientPoolGet(t *testing.T) {
	size := 1
	p := newTestPool(size, time.Second, t)
	defer p.Close()

	for i := 0; i < 10; i++ {
		c, err := p.Get()
		assert.Nil(t, err)
		p.Put(c)
	}
	assert.Equal(t, size, len(p.connections))
}

func TestClientPoolGetMax(t *testing.T) {
	size := 3
	p := newTestPool(size, time.Second, t)
	defer p.Close()

	clients := make([]client.ImmuClient, 0, size)
	for i := 0; i < size; i++ {
		c, err := p.Get()
		assert.Nil(t, err)
		clients = append(clients, c)
	}
	for i := 0; i < size; i++ {
		p.Put(clients[i])
	}
	assert.Equal(t, size, len(p.connections))
}

func TestClientPoolGreaterThanMax(t *testing.T) {
	size := 3
	p := newTestPool(size, time.Second, t)
	defer p.Close()

	clients := make([]client.ImmuClient, 0, size)
	for i := 0; i < size; i++ {
		c, err := p.Get()
		assert.Nil(t, err)
		clients = append(clients, c)
	}

	doneCh := make(chan struct{})

	// thread #1
	go func() {
		_, err := p.Get()
		assert.Nil(t, err)
		doneCh <- struct{}{}
	}()

	var err error
	select {
	case <-doneCh:
	case <-time.After(20 * time.Millisecond): // call to get should block when there are no connections in the connection pool
		err = errTimedOut
	}
	assert.Equal(t, errTimedOut, err)

	// return the connections back to the pool, should unblock thread #1
	for i := 0; i < size; i++ {
		p.Put(clients[i])
	}
	<-doneCh

	// thread #1 already has a client which is not returned
	assert.Equal(t, size-1, len(p.connections))
}

func TestClientPoolGetMaxConcurrently(t *testing.T) {
	size := 5
	p := newTestPool(size, time.Second, t)
	defer p.Close()

	clients := make([]client.ImmuClient, 0, size)

	wg := sync.WaitGroup{}
	for i := 0; i < size; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			c, err := p.Get()
			assert.Nil(t, err)
			clients = append(clients, c)
		}()
	}
	wg.Wait()
	assert.Equal(t, size, p.total)

	// add another get call to pool
	doneCh := make(chan struct{})
	go func() {
		_, err := p.Get()
		assert.Nil(t, err)
		doneCh <- struct{}{}
	}()

	// should not increment total value
	assert.Equal(t, size, p.total)

	// return the connections back to the pool
	for i := 0; i < size; i++ {
		p.Put(clients[i])
	}
	<-doneCh

	assert.Equal(t, size-1, len(p.connections))
}

func TestClientPoolClose(t *testing.T) {
	size := 5
	p := newTestPool(size, time.Second, t)
	defer p.Close()

	clients := make([]client.ImmuClient, 0, size)
	for i := 0; i < size; i++ {
		c, err := p.Get()
		assert.Nil(t, err)
		clients = append(clients, c)
	}

	// return the connections back to the pool
	for i := 0; i < size; i++ {
		p.Put(clients[i])
	}

	assert.Equal(t, size, len(p.connections))

	assert.Nil(t, p.Close())
	assert.Nil(t, p.Close())

	assert.Equal(t, 0, len(p.connections))
}

func TestClientPoolConnectionTimeout(t *testing.T) {
	size := 1
	cnf := &Config{MaxConnections: size, Heartbeat: time.Second}
	p := NewClientPool(cnf)
	p.init = func() (client.ImmuClient, error) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		defer cancel()
		<-ctx.Done()
		m := client.NewMockImmuClient(ctrl)
		return m, nil
	}

	// add a get call to pool
	doneCh := make(chan struct{})
	go func() {
		_, err := p.Get()
		assert.Nil(t, err)
		doneCh <- struct{}{}
	}()

	var err error
	select {
	case <-doneCh:
	case <-time.After(30 * time.Millisecond): // call to get should block when there are no connections in the connection pool
		err = errTimedOut
	}
	assert.Equal(t, errTimedOut, err)
}

func TestClientPoolDo(t *testing.T) {
	size := 1
	p := newTestPool(size, time.Second, t)
	defer p.Close()

	t.Run("do without an error", func(t *testing.T) {
		assert.Nil(t, p.Do(func(client.ImmuClient) error {
			return nil
		}))
	})

	t.Run("do with an error", func(t *testing.T) {
		assert.NotNil(t, p.Do(func(client.ImmuClient) error {
			return errors.New("fail")
		}))
	})
}

func TestClientPoolDoWithTimeout(t *testing.T) {
	size := 1
	cnf := &Config{MaxConnections: size, Heartbeat: time.Second}
	p := NewClientPool(cnf)
	p.init = func() (client.ImmuClient, error) {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
		defer cancel()
		<-ctx.Done()
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		return client.NewMockImmuClient(ctrl), ctx.Err()
	}

	t.Run("do with a timeout error", func(t *testing.T) {
		assert.Equal(t, context.DeadlineExceeded, p.Do(func(client.ImmuClient) error {
			return nil
		}))
	})
}

func TestNewClientPool(t *testing.T) {
	size := 1
	cnf := &Config{MaxConnections: size, Heartbeat: time.Second}
	p := NewClientPool(cnf)
	cli, err := p.Get()
	// client should throw an error as server is not running on default port
	assert.Error(t, err)
	assert.Nil(t, cli)
}

func TestClientPoolHeartbeat(t *testing.T) {
	size := 5
	cnf := &Config{MaxConnections: size}
	p := NewClientPool(cnf)
	p.init = func() (client.ImmuClient, error) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		m := client.NewMockImmuClient(ctrl)
		m.EXPECT().Health(context.TODO()).Return(nil, errTimedOut).AnyTimes()
		m.EXPECT().CloseSession(context.Background()).Return(nil).AnyTimes()
		m.EXPECT().Disconnect().Return(nil).AnyTimes()
		return m, nil
	}

	clients := make([]client.ImmuClient, 0, size)
	for i := 0; i < size; i++ {
		c, err := p.Get()
		assert.Nil(t, err)
		clients = append(clients, c)
	}

	// return the connections back to the pool
	for i := 0; i < size; i++ {
		p.Put(clients[i])
	}
	assert.Equal(t, size, len(p.connections))

	// healtcheck should remove all faulty connections, as heartbeat would fail
	p.healthCheck()
	assert.Equal(t, 0, len(p.connections))
}

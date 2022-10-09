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
	"testing"
	"time"

	"github.com/codenotary/immudb/pkg/client"
	gomock "github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
)

var (
	newDBTestPool = func(dbs []string, size int, timeout time.Duration, t *testing.T) *DatabasePool {
		p := NewDatabasePool("", 0, dbs, timeout, size, nil)
		for _, c := range p.connections {
			c.init = func() (client.ImmuClient, error) {
				ctrl := gomock.NewController(t)
				defer ctrl.Finish()
				m := client.NewMockImmuClient(ctrl)
				m.EXPECT().CloseSession(context.Background()).Return(nil).AnyTimes()
				m.EXPECT().Disconnect().Return(nil).AnyTimes()
				return m, nil
			}
		}
		return p
	}
)

func TestNewDatabasePoolSet(t *testing.T) {
	size := 1
	dbs := []string{"db1", "db2"}
	p := newDBTestPool(dbs, size, time.Second, t)
	defer p.Close()

	assert.Equal(t, len(dbs), len(p.connections))

	assert.Equal(t, p.With("db1").conf.Db, "db1")
	assert.Equal(t, p.With("db1").conf.Options.Database, "db1")
	assert.Equal(t, p.With("db2").conf.Db, "db2")
	assert.Equal(t, p.With("db2").conf.Options.Database, "db2")
}

func TestDatabasePoolGet(t *testing.T) {
	size := 1
	dbs := []string{"db1", "db2"}
	p := newDBTestPool(dbs, size, time.Second, t)
	defer p.Close()

	assert.Equal(t, len(dbs), len(p.connections))
	assert.NotNil(t, p.With("db1"))

	db1 := p.With("db1")
	for i := 0; i < 10; i++ {
		c, err := db1.Get()
		assert.Nil(t, err)
		db1.Put(c)
	}
	assert.Equal(t, size, len(db1.connections))
}

func TestDatabasePoolGetMax(t *testing.T) {
	size := 3
	dbs := []string{"db1", "db2"}
	p := newDBTestPool(dbs, size, time.Second, t)
	defer p.Close()

	db1 := p.With("db1")
	clients := make([]client.ImmuClient, 0, size)
	for i := 0; i < size; i++ {
		c, err := db1.Get()
		assert.Nil(t, err)
		clients = append(clients, c)
	}
	for i := 0; i < size; i++ {
		db1.Put(clients[i])
	}
	assert.Equal(t, size, len(db1.connections))
}

func TestDatabasePoolDo(t *testing.T) {
	size := 1
	dbs := []string{"db1", "db2"}
	p := newDBTestPool(dbs, size, 30*time.Millisecond, t)
	defer p.Close()

	t.Run("do without an error", func(t *testing.T) {
		assert.Nil(t, p.With("db1").Do(func(client.ImmuClient) error {
			return nil
		}))
	})

	t.Run("do with an error", func(t *testing.T) {
		assert.NotNil(t, p.With("db1").Do(func(client.ImmuClient) error {
			return errors.New("fail")
		}))
	})
}

func TestDatabasePoolDoWith(t *testing.T) {
	size := 1
	dbs := []string{"db1", "db2"}
	p := newDBTestPool(dbs, size, 30*time.Millisecond, t)
	defer p.Close()

	t.Run("do without an error", func(t *testing.T) {
		assert.Nil(t, p.DoWith("db1", func(client.ImmuClient) error {
			return nil
		}))
	})

	t.Run("do with an error", func(t *testing.T) {
		assert.NotNil(t, p.DoWith("db2", func(client.ImmuClient) error {
			return errors.New("fail")
		}))
	})
}

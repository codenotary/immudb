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
	"testing"
	"time"

	"github.com/codenotary/immudb/pkg/client"
	"github.com/stretchr/testify/assert"
)

func TestConfig(t *testing.T) {
	c := &Config{}

	// set options
	c.WithOptions(client.DefaultOptions())

	// set db
	c.WithDb("foo")
	assert.Equal(t, c.Db, "foo")
	assert.Equal(t, c.Options.Database, "foo")

	// set address
	c.WithAddress("127.0.0.1")
	assert.Equal(t, c.Address, "127.0.0.1")
	assert.Equal(t, c.Options.Address, "127.0.0.1")

	// set port
	c.WithPort(1)
	assert.Equal(t, c.Port, 1)
	assert.Equal(t, c.Options.Port, 1)

	// set heartbeat timeout
	c.WithHeatbeat(time.Second)
	assert.Equal(t, c.Heartbeat, time.Second)

	// set max connections
	c.WithMaxConnections(5)
	assert.Equal(t, c.MaxConnections, 5)
}

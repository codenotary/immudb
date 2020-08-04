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

package store

import (
	"os"
	"testing"

	"github.com/codenotary/immudb/pkg/logger"

	"github.com/stretchr/testify/assert"
)

func TestNewTreeStore(t *testing.T) {

	db := makeBadger()
	defer db.Close()
	log := logger.NewSimpleLoggerWithLevel("test", os.Stderr, logger.LogDebug)

	ts, err := newTreeStore(db.DB, 1000, true, log)
	assert.NoError(t, err)

	assert.Zero(t, ts.Width())

	// add two items
	ts.Commit(ts.NewEntry([]byte("key"), []byte("value")))
	ts.Commit(ts.NewEntry([]byte("key"), []byte("value")))

	ts.WaitUntil(1) // second item at idx 1

	assert.Equal(t, uint64(2), ts.Width())

	ts.Close()
	db.Restart()

	ts, err = newTreeStore(db.DB, 1000, true, log)
	assert.NoError(t, err)

	assert.Equal(t, uint64(2), ts.Width())

	ts.Close()
}

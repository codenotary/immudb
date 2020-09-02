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
package tbtree

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSnapshotSerialization(t *testing.T) {
	tbtree, err := Open("tbtree.idb", DefaultOptions().setMaxNodeSize(MinNodeSize))
	assert.NoError(t, err)
	defer os.Remove("tbtree.idb")

	//randomInsertions(t, tbtree, 10, true)
	monotonicInsertions(t, tbtree, 1, 10, true)

	snapshot, err := tbtree.Snapshot()
	assert.NotNil(t, snapshot)
	assert.NoError(t, err)

	err = snapshot.WriteTo(tbtree.f, true, true)
	assert.NoError(t, err)

	err = snapshot.Close()
	assert.NoError(t, err)

	err = tbtree.Close()
	assert.NoError(t, err)
}

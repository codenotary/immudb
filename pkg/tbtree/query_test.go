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
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestReaderForEmptyTreeShouldReturnError(t *testing.T) {
	tbtree, _ := New()

	root, err := tbtree.Root()
	assert.NotNil(t, root)
	assert.NoError(t, err)

	_, err = root.Reader(&ReaderSpec{prefix: []byte{0, 0, 0, 0}, ascOrder: true})
	assert.Equal(t, ErrNoMoreEntries, err)
}

func TestReaderAscendingScan(t *testing.T) {
	tbtree, err := NewWith(DefaultOptions().setMaxNodeSize(MinNodeSize))
	assert.NoError(t, err)

	monotonicInsertions(t, tbtree, 1, 1000, true)

	root, err := tbtree.Root()
	assert.NotNil(t, root)
	assert.NoError(t, err)

	rspec := &ReaderSpec{
		prefix:      []byte{0, 0, 0, 250},
		matchPrefix: true,
		ascOrder:    true,
	}
	reader, err := root.Reader(rspec)
	assert.NoError(t, err)

	for {
		k, _, _, err := reader.Read()
		if err != nil {
			assert.Equal(t, ErrNoMoreEntries, err)
			break
		}

		assert.True(t, bytes.Compare(reader.prefix, k) < 1)
	}
}

func TestReaderDescendingScan(t *testing.T) {
	tbtree, err := NewWith(DefaultOptions().setMaxNodeSize(MinNodeSize))
	assert.NoError(t, err)

	monotonicInsertions(t, tbtree, 1, 257, true)

	root, err := tbtree.Root()
	assert.NotNil(t, root)
	assert.NoError(t, err)

	rspec := &ReaderSpec{
		prefix:      []byte{0, 0, 0, 100},
		matchPrefix: false,
		ascOrder:    false,
	}
	reader, err := root.Reader(rspec)
	assert.NoError(t, err)

	for {
		k, _, _, err := reader.Read()
		if err != nil {
			assert.Equal(t, ErrNoMoreEntries, err)
			break
		}

		assert.True(t, bytes.Compare(k, reader.prefix) < 1)
	}
}

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

package schema

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestItem_Hash(t *testing.T) {
	i := Item{
		Key:   []byte(`key`),
		Value: []byte(`val`),
		Index: 0,
	}
	b := i.Hash()
	assert.Len(t, b, 32)

	json, err := i.MarshalJSON()
	assert.IsType(t, []byte{}, json)
	assert.Nil(t, err)

	var in *Item
	h := in.Hash()
	assert.Nil(t, h)

}

func TestStructuredItem_Hash(t *testing.T) {
	i := StructuredItem{
		Key: []byte(`key`),
		Value: &Content{
			Timestamp: 0,
			Payload:   []byte(`val`),
		},
		Index: 0,
	}
	b, err := i.Hash()
	assert.Nil(t, err)
	assert.Len(t, b, 32)

	json, err := i.MarshalJSON()
	assert.IsType(t, []byte{}, json)
	assert.Nil(t, err)

	var in *StructuredItem
	h, err := in.Hash()
	assert.Nil(t, h)
	assert.Error(t, err)
}

func TestSafeItem_Hash(t *testing.T) {
	i := SafeItem{
		Item: &Item{
			Key:   []byte(`key`),
			Value: []byte(`val`),
			Index: 0,
		},
		Proof: &Proof{
			Leaf:            []byte(`leaf`),
			Index:           0,
			Root:            []byte(`root`),
			At:              0,
			InclusionPath:   nil,
			ConsistencyPath: nil,
		},
	}
	b, err := i.Hash()
	assert.Nil(t, err)
	assert.Len(t, b, 32)

	var in *SafeItem
	h, err := in.Hash()
	assert.Nil(t, h)
	assert.Error(t, err)
}

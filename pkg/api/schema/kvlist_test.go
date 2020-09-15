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

func TestKVList_Validate(t *testing.T) {
	kv := &KVList{
		KVs: []*KeyValue{
			{
				Key:   []byte("key1"),
				Value: []byte("val1"),
			},
			{
				Key:   []byte("key2"),
				Value: []byte("val2"),
			},
			{
				Key:   []byte("key3"),
				Value: []byte("val3"),
			},
		},
	}
	err := kv.Validate()
	assert.NoError(t, err)
}

func TestKVList_ValidateDuplicateKeys(t *testing.T) {
	kv := &KVList{
		KVs: []*KeyValue{
			{
				Key:   []byte("key1"),
				Value: []byte("val1"),
			},
			{
				Key:   []byte("key1"),
				Value: []byte("val2"),
			},
			{
				Key:   []byte("key3"),
				Value: []byte("val3"),
			},
		},
	}
	err := kv.Validate()
	assert.Errorf(t, err, "duplicate keys are not supported in single batch transaction")
}
func TestKVList_ValidateEmptySet(t *testing.T) {
	kv := &KVList{
		KVs: []*KeyValue{},
	}
	err := kv.Validate()
	assert.Errorf(t, err, "empty set")
}

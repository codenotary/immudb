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
	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/dgraph-io/badger/v2"
)

func itemToSchema(key []byte, item *badger.Item) (*schema.Item, error) {
	value, err := item.ValueCopy(nil)
	if err != nil {
		return nil, mapError(err)
	}
	if key == nil || len(key) == 0 {
		key = item.KeyCopy(key)
	}

	v, ts := UnwrapValueWithTS(value)

	return &schema.Item{
		Key:   key,
		Value: v,
		Index: ts - 1,
	}, nil
}

func checkKey(key []byte) error {
	if len(key) == 0 || isReservedKey(key) {
		return ErrInvalidKey
	}
	return nil
}

func checkSet(key []byte) error {
	if len(key) == 0 || isReservedKey(key) {
		return ErrInvalidSet
	}
	return nil
}

func checkReference(key []byte) error {
	if len(key) == 0 || isReservedKey(key) {
		return ErrInvalidReference
	}
	return nil
}

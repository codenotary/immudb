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
	"encoding/json"
	"errors"

	"github.com/codenotary/immudb/pkg/api"
)

// Hash returns the computed hash of _Item_.
func (i *Item) Hash() []byte {
	if i == nil {
		return nil
	}
	d := api.Digest(i.Index, i.Key, i.Value)
	return d[:]
}

// Hash computes and returns the hash of the structured item
func (si *StructuredItem) Hash() ([]byte, error) {
	if si == nil {
		return nil, errors.New("Empty pointer receive")
	}
	i, err := si.ToItem()
	if err != nil {
		return nil, err
	}
	d := api.Digest(i.Index, i.Key, i.Value)
	return d[:], nil
}

// Hash computes and returns the hash of the safe item
func (s *SafeItem) Hash() ([]byte, error) {
	if s == nil {
		return nil, errors.New("Empty pointer receive")
	}
	d := api.Digest(s.Item.Index, s.Item.Key, s.Item.Value)
	return d[:], nil
}

// MarshalJSON marshals the item to JSON
func (m *Item) MarshalJSON() ([]byte, error) {
	type Ci Item
	return json.Marshal(&struct {
		Index uint64 `protobuf:"varint,3,opt,name=index,proto3" json:"index"`
		*Ci
	}{
		Index: m.Index,
		Ci:    (*Ci)(m),
	})
}

// MarshalJSON marshals the structured item to JSON
func (m *StructuredItem) MarshalJSON() ([]byte, error) {
	type Ci StructuredItem
	return json.Marshal(&struct {
		Index uint64 `protobuf:"varint,3,opt,name=index,proto3" json:"index"`
		*Ci
	}{
		Index: m.Index,
		Ci:    (*Ci)(m),
	})
}

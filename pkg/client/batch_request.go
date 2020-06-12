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

package client

import (
	"io"
	"io/ioutil"

	"github.com/codenotary/immudb/pkg/api/schema"
)

// BatchRequest batch request payload
type BatchRequest struct {
	Keys   []io.Reader
	Values []io.Reader
}

func (b *BatchRequest) toKVList() (*schema.KVList, error) {
	list := &schema.KVList{}
	for i := range b.Keys {
		key, err := ioutil.ReadAll(b.Keys[i])
		if err != nil {
			return nil, err
		}
		value, err := ioutil.ReadAll(b.Values[i])
		if err != nil {
			return nil, err
		}
		list.KVs = append(list.KVs, &schema.KeyValue{
			Key:   key,
			Value: value,
		})
	}
	return list, nil
}

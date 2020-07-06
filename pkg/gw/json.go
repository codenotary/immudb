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

package gw

import (
	"encoding/json"
)

// JSON ...
type JSON interface {
	Marshal(interface{}) ([]byte, error)
	Unmarshal([]byte, interface{}) error
}

type defaultJSON struct {
	marshal   func(interface{}) ([]byte, error)
	unmarshal func([]byte, interface{}) error
}

// DefaultJSON ...
func DefaultJSON() JSON {
	return newDefaultJSON()
}

func newDefaultJSON() *defaultJSON {
	return &defaultJSON{
		marshal:   json.Marshal,
		unmarshal: json.Unmarshal,
	}
}

func (dj *defaultJSON) Marshal(v interface{}) ([]byte, error) {
	return dj.marshal(v)
}

func (dj *defaultJSON) Unmarshal(data []byte, v interface{}) error {
	return dj.unmarshal(data, v)
}

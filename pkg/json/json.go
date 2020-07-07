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

package json

import (
	"encoding/json"
)

// JSON ...
type JSON interface {
	Marshal(interface{}) ([]byte, error)
	Unmarshal([]byte, interface{}) error
}

// StandardJSON ...
type StandardJSON struct {
	MarshalF   func(interface{}) ([]byte, error)
	UnmarshalF func([]byte, interface{}) error
}

// DefaultJSON ...
func DefaultJSON() *StandardJSON {
	return &StandardJSON{
		MarshalF:   json.Marshal,
		UnmarshalF: json.Unmarshal,
	}
}

// Marshal ...
func (sj *StandardJSON) Marshal(v interface{}) ([]byte, error) {
	return sj.MarshalF(v)
}

// Unmarshal ...
func (sj *StandardJSON) Unmarshal(data []byte, v interface{}) error {
	return sj.UnmarshalF(data, v)
}

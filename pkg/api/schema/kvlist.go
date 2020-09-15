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
	"encoding/base64"
	"errors"
)

// Validate checks for duplicated keys values that are not supported at the moment.
func (l KVList) Validate() error {
	if len(l.GetKVs()) == 0 {
		return errors.New("empty set")
	}
	m := make(map[string]struct{}, len(l.GetKVs()))
	for _, k := range l.GetKVs() {
		b64k := base64.StdEncoding.EncodeToString(k.Key)
		if _, ok := m[b64k]; ok {
			return errors.New("duplicate keys are not supported in single batch transaction")
		}
		m[b64k] = struct{}{}
	}
	return nil
}

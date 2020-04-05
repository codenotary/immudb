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

func (si *StructuredItem) Hash() []byte {
	if si == nil {
		return nil
	}
	i := si.ToItem()
	d := api.Digest(i.Index, i.Key, i.Value)
	return d[:]
}

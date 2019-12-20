/*
Copyright 2019 vChain, Inc.

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
	"github.com/codenotary/immustore/pkg/api"
	"github.com/codenotary/immustore/pkg/tree"
)

func (t *Store) InclusionProof(index uint64) (*api.InclusionProof, error) {

	ts := t.tree
	ts.RLock()
	defer ts.RUnlock()

	leaf := ts.Get(0, index)
	if leaf == nil {
		return nil, IndexNotFoundErr
	}

	return &api.InclusionProof{
		Index: index,
		Hash:  *leaf,

		Root: tree.Root(ts),
		At:   ts.w - 1,

		Path: tree.PathAt(ts, ts.w-1, index),
	}, nil
}

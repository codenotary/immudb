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
	"github.com/codenotary/immudb/pkg/tree"
)

func (s *Store) InclusionProof(index schema.Index) (*schema.InclusionProof, error) {

	ts := s.tree
	ts.RLock()
	defer ts.RUnlock()

	leaf := ts.Get(0, index.Index)
	if leaf == nil {
		return nil, IndexNotFoundErr
	}

	root := tree.Root(ts)

	path := tree.InclusionProof(ts, ts.w-1, index.Index)

	return &schema.InclusionProof{
		Index: index.Index,
		Leaf:  leaf[:],

		Root: root[:],
		At:   ts.w - 1,

		Path: path.ToSlice(),
	}, nil
}

func (s *Store) ConsistencyProof(index schema.Index) (*schema.ConsistencyProof, error) {

	ts := s.tree
	ts.RLock()
	defer ts.RUnlock()

	at := ts.w - 1
	if index.Index > at {
		return nil, IndexNotFoundErr
	}

	root := tree.Root(ts)

	path := tree.ConsistencyProof(ts, ts.w-1, index.Index)

	return &schema.ConsistencyProof{
		First:      index.Index,
		Second:     at,
		SecondRoot: root[:],

		Path: path.ToSlice(),
	}, nil
}

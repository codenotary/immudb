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

// A treeStorePQ implements heap.Interface and holds *treeStoreEntry(s).
type treeStorePQ []*treeStoreEntry

func (pq treeStorePQ) Len() int           { return len(pq) }
func (pq treeStorePQ) Less(i, j int) bool { return pq[i].ts < pq[j].ts }
func (pq treeStorePQ) Swap(i, j int)      { pq[i], pq[j] = pq[j], pq[i] }

func (pq *treeStorePQ) Push(x interface{}) { *pq = append(*pq, x.(*treeStoreEntry)) }

func (pq *treeStorePQ) Pop() interface{} {
	old := *pq
	n := len(old)
	item := old[n-1]
	old[n-1] = nil // avoid memory leak
	*pq = old[0 : n-1]
	return item
}

func (pq treeStorePQ) Min() uint64 {
	if l := len(pq); l > 0 {
		return pq[0].ts
	}
	return 0
}

/*
Copyright 2025 Codenotary Inc. All rights reserved.

SPDX-License-Identifier: BUSL-1.1
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    https://mariadb.com/bsl11/

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package tbtree

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSnapshotSet(t *testing.T) {
	type test struct {
		name       string
		localRange numRange
		mainRange  numRange
	}

	n := 100

	cases := []test{
		{
			name: "disjoint ranges - main first",
			mainRange: numRange{
				start: 0,
				end:   n - 1,
			},
			localRange: numRange{
				start: n,
				end:   2*n - 1,
			},
		},
		{
			name: "disjoint ranges - local first",
			localRange: numRange{
				start: 0,
				end:   n - 1,
			},
			mainRange: numRange{
				start: n,
				end:   2*n - 1,
			},
		},
		{
			name: "overlapping ranges from start",
			mainRange: numRange{
				start: n / 2,
				end:   2*n - 1,
			},
			localRange: numRange{
				start: 0,
				end:   n,
			},
		},
		{
			name: "overlapping ranges from end",
			mainRange: numRange{
				start: 0,
				end:   n - 1,
			},
			localRange: numRange{
				start: n / 2,
				end:   n + n/2,
			},
		},
		{
			name: "local replaces all range",
			mainRange: numRange{
				start: 0,
				end:   n - 1,
			},
			localRange: numRange{
				start: 0,
				end:   n - 1,
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			writeBufferSize := 5 * 1024 * 1024
			pageBufferSize := 1024 * 1024

			tree, err := newTBTree(writeBufferSize, pageBufferSize)
			require.NoError(t, err)

			tc.mainRange.forEach(func(n int) {
				err := tree.Insert(Entry{
					Ts:    uint64(1),
					HOff:  OffsetNone,
					Key:   binary.BigEndian.AppendUint32(nil, uint32(n)),
					Value: []byte(fmt.Sprintf("value-%d", n)),
				})
				require.NoError(t, err)
			})

			err = tree.FlushReset()
			require.NoError(t, err)

			snap, err := tree.WriteSnapshot()
			require.NoError(t, err)
			defer snap.Close()

			tc.localRange.forEach(func(n int) {
				key := binary.BigEndian.AppendUint32(nil, uint32(n))
				err := snap.Set(key, []byte(fmt.Sprintf("new-value-%d", n)))
				require.NoError(t, err)
			})

			u := tc.mainRange.union(tc.localRange)
			u.forEach(func(n int) {
				key := binary.BigEndian.AppendUint32(nil, uint32(n))
				v, ts, hc, err := snap.Get(key)
				require.NoError(t, err)

				if tc.localRange.contains(n) {
					expectedHC := uint64(1)
					if tc.mainRange.contains(n) {
						expectedHC++
					}
					require.Equal(t, uint64(2), ts)
					require.Equal(t, expectedHC, hc)
					require.Equal(t, []byte(fmt.Sprintf("new-value-%d", n)), v)
				} else {
					require.Equal(t, []byte(fmt.Sprintf("value-%d", n)), v)
					require.Equal(t, uint64(1), ts)
					require.Equal(t, uint64(1), hc)
				}
			})

			t.Run("forward iteration", func(t *testing.T) {
				seekAt := u.start + rand.Intn(u.end-u.start+1)
				it, err := snap.NewIterator(DefaultIteratorOptions())
				require.NoError(t, err)

				err = it.Seek(binary.BigEndian.AppendUint32(nil, uint32(seekAt)))
				require.NoError(t, err)

				m := seekAt
				for {
					e, err := it.Next()
					if errors.Is(err, ErrNoMoreEntries) {
						break
					}

					if tc.localRange.contains(m) {
						expectedHC := uint64(0)
						if tc.mainRange.contains(m) {
							expectedHC++
						}
						require.Equal(t, uint64(2), e.Ts)
						require.Equal(t, expectedHC, e.HC)
						require.Equal(t, []byte(fmt.Sprintf("new-value-%d", m)), e.Value)
					} else {
						require.Equal(t, uint64(1), e.Ts)
						require.Equal(t, uint64(0), e.HC)
						require.Equal(t, []byte(fmt.Sprintf("value-%d", m)), e.Value)
					}
					m++
				}
				require.Equal(t, u.end+1, m)
			})

			t.Run("backward iteration", func(t *testing.T) {
				seekAt := u.start + rand.Intn(u.end-u.start+1)

				opts := DefaultIteratorOptions()
				opts.Reversed = true

				it, err := snap.NewIterator(opts)
				require.NoError(t, err)

				err = it.Seek(binary.BigEndian.AppendUint32(nil, uint32(seekAt)))
				require.NoError(t, err)

				m := seekAt
				for {
					e, err := it.Next()
					if errors.Is(err, ErrNoMoreEntries) {
						break
					}

					if tc.localRange.contains(m) {
						expectedHC := uint64(0)
						if tc.mainRange.contains(m) {
							expectedHC++
						}
						require.Equal(t, uint64(2), e.Ts)
						require.Equal(t, expectedHC, e.HC)
						require.Equal(t, []byte(fmt.Sprintf("new-value-%d", m)), e.Value)
					} else {
						require.Equal(t, uint64(1), e.Ts)
						require.Equal(t, uint64(0), e.HC)
						require.Equal(t, []byte(fmt.Sprintf("value-%d", m)), e.Value)
					}
					m--
				}
				require.Equal(t, -1, m)
			})
		})
	}
}

type numRange struct {
	start int
	end   int
}

func (r *numRange) contains(n int) bool {
	return n >= r.start && n <= r.end
}

func (r *numRange) union(other numRange) numRange {
	return numRange{
		start: min(r.start, other.start),
		end:   max(r.end, other.end),
	}
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func (r *numRange) forEach(each func(n int)) {
	for n := r.start; n <= r.end; n++ {
		each(n)
	}
}

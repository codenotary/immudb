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

package container

type Stack[T any] struct {
	elems []T
}

func NewStack[T any](capacity int) *Stack[T] {
	return &Stack[T]{
		elems: make([]T, 0, capacity),
	}
}

func (s *Stack[T]) Push(x T) bool {
	s.elems = append(s.elems, x)
	return true
}

func (s *Stack[T]) Pop() (T, bool) {
	if len(s.elems) == 0 {
		var x T
		return x, false
	}

	n := s.Len()
	e := s.elems[n-1]

	s.elems = s.elems[:n-1]
	return e, true
}

func (s *Stack[T]) Len() int {
	return len(s.elems)
}

func (s *Stack[T]) Discard(n int) {
	if len(s.elems) <= n {
		s.Reset()
		return
	}
	s.elems = s.elems[:len(s.elems)-n]
}

func (s *Stack[T]) Reset() {
	s.elems = nil
}

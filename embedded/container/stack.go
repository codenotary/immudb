package container

const MaxStackSize = 100

type FixedSizeStack[T any] struct {
	elems [MaxStackSize]T
	next  int
}

func (s *FixedSizeStack[T]) Push(x T) bool {
	if s.next >= len(s.elems) {
		return false
	}

	s.elems[s.next] = x
	s.next++
	return true
}

func (s *FixedSizeStack[T]) Pop() (T, bool) {
	if s.next <= 0 {
		var x T
		return x, false
	}

	e := s.elems[s.next-1]
	s.next--

	return e, true
}

func (s *FixedSizeStack[T]) Len() int {
	return s.next
}

func (s *FixedSizeStack[T]) Reset() {
	s.next = 0
}

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

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

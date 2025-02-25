package container

type Ordered interface {
	Less(other any) bool
}

type PriorityQueue[T Ordered] []T

func (h PriorityQueue[T]) Len() int { return len(h) }

func (h PriorityQueue[T]) Less(i, j int) bool {
	return h[i].Less(h[j])
}

func (h PriorityQueue[T]) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
}

func (h *PriorityQueue[T]) Push(x any) {
	*h = append(*h, x.(T))
}

func (h PriorityQueue[T]) Top() any {
	if h.Len() == 0 {
		return nil
	}
	return h[0]
}

func (h *PriorityQueue[T]) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

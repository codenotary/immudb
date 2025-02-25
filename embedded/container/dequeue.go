package container

type Dequeue[T any] struct {
	start int
	end   int
	n     int
	data  []T
}

func NewDequeue[T any](capacity int) *Dequeue[T] {
	return &Dequeue[T]{
		data: make([]T, capacity),
	}
}

func (q *Dequeue[T]) PushBack(x T) {
	q.resize()
	q.data[q.end] = x
	q.end = (q.end + 1) % len(q.data)

	q.n++
}

func (q *Dequeue[T]) PushFront(x T) {
	q.resize()
	q.start = (q.start - 1 + len(q.data)) % len(q.data)
	q.data[q.start] = x

	q.n++
}

func (q *Dequeue[T]) PopFront() (T, bool) {
	if q.n == 0 {
		var x T
		return x, false
	}

	x := q.data[q.start]
	q.start = (q.start + 1) % len(q.data)

	q.n--

	return x, true
}

func (q *Dequeue[T]) PopBack() (T, bool) {
	if q.n == 0 {
		var x T
		return x, false
	}

	q.end = (q.end - 1 + len(q.data)) % len(q.data)
	x := q.data[q.end]

	q.n--

	return x, true
}

func (q *Dequeue[T]) Front() (T, bool) {
	if q.n == 0 {
		var x T
		return x, false
	}

	return q.data[q.start], true
}

func (q *Dequeue[T]) Back() (T, bool) {
	if q.n == 0 {
		var x T
		return x, false
	}
	return q.data[q.end], true
}

func (q *Dequeue[T]) resize() {
	if q.n < len(q.data) {
		return
	}

	newData := make([]T, 2*len(q.data))
	for n := q.n; n >= 0; n-- {
		newData[n] = q.data[(q.start+n)%len(q.data)]
	}

	q.start = 0
	q.end = q.n
	q.data = newData
}

func (q *Dequeue[T]) Cap() int {
	return len(q.data)
}

func (q *Dequeue[T]) Len() int {
	return q.n
}

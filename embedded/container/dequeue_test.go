package container

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDequePushPop(t *testing.T) {
	q := NewDequeue[int](10)

	numElements := 100
	for i := numElements / 2; i < numElements; i++ {
		q.PushBack(i)
	}

	for i := numElements/2 - 1; i >= 0; i-- {
		q.PushFront(i)
	}

	require.Equal(t, q.Len(), numElements)

	for n := 0; n < numElements/2; n++ {
		e, ok := q.PopFront()
		require.True(t, ok)
		require.Equal(t, n, e)
	}

	for n := numElements - 1; n >= numElements/2; n-- {
		e, ok := q.PopBack()
		require.True(t, ok)
		require.Equal(t, n, e)
	}

	require.Equal(t, q.Len(), 0)
}

func TestDequeReverse(t *testing.T) {
	q := NewDequeue[int](10)

	numElements := 100
	for i := 0; i < numElements; i++ {
		q.PushBack(i)
	}

	for n := 0; n < numElements; n++ {
		e, ok := q.PopFront()
		require.True(t, ok)

		q.PushBack(e)
	}
	require.Equal(t, q.Len(), numElements)

	for n := 0; n < numElements; n++ {
		e, ok := q.PopFront()
		require.True(t, ok)
		require.Equal(t, n, e)
	}
}

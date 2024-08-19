package semaphore

import "sync/atomic"

type Semaphore struct {
	maxWeight  uint64
	currWeight uint64
}

func New(maxWeight uint64) *Semaphore {
	return &Semaphore{
		maxWeight:  maxWeight,
		currWeight: 0,
	}
}

func (m *Semaphore) Acquire(n uint64) bool {
	if newVal := atomic.AddUint64(&m.currWeight, n); newVal <= m.maxWeight {
		return true
	}
	m.Release(n)
	return false
}

func (m *Semaphore) Release(n uint64) {
	atomic.AddUint64(&m.currWeight, ^uint64(n-1))
}

func (m *Semaphore) Value() uint64 {
	return atomic.LoadUint64(&m.currWeight)
}

func (m *Semaphore) MaxWeight() uint64 {
	return m.maxWeight
}

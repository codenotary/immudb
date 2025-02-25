package latch

import (
	"runtime"
	"sync/atomic"
)

const defaultMaxSpins = 1000

type LWLock struct {
	count uint64
}

func (lw *LWLock) Lock() {
	for !lw.TryLock() {
	}
}

func (lw *LWLock) TryLock() bool {
	return atomic.CompareAndSwapUint64(&lw.count, 0, 1)
}

func (lw *LWLock) Unlock() {
	new := atomic.AddUint64(&lw.count, ^uint64(0))
	if new != 0 {
		panic("Unlock() was called but no exclusive lock was held")
	}
}

func (lw *LWLock) Downgrade() {
	if !atomic.CompareAndSwapUint64(&lw.count, 1, 0x2) {
		panic("Downgrade() was called but no exclusive lock was held")
	}
}

func (lw *LWLock) TryRLock() bool {
	for {
		val := atomic.LoadUint64(&lw.count)
		if val&0x1 != 0 {
			return false
		}

		if atomic.CompareAndSwapUint64(&lw.count, val, val+0x2) {
			return true
		}
	}
}

func (lw *LWLock) RLock() {
	if lw.TryRLockWithSpin(defaultMaxSpins) {
		return
	}

	for !lw.TryRLock() {
		runtime.Gosched()
	}
}

func (lw *LWLock) TryRLockWithSpin(maxSpins uint64) bool {
	for nSpins := uint64(0); nSpins < maxSpins; nSpins++ {
		if lw.TryRLock() {
			return true
		}
	}
	return false
}

func (lw *LWLock) RUnlock() {
	new := atomic.AddUint64(&lw.count, ^uint64(1))
	if new >= new+2 {
		panic("RUnlock() was called but no read lock was held")
	}
}

func (lw *LWLock) Readers() int {
	w := atomic.LoadUint64(&lw.count)
	return int(w >> 1)
}

func (lw *LWLock) Free() bool {
	w := atomic.LoadUint64(&lw.count)
	return w == 0
}

package store

type AdaptiveBuffer struct {
	buf []byte

	usedBytes int
	n         int
	chunkSize int
}

func NewResizableBuffer(chunkSize int) *AdaptiveBuffer {
	return &AdaptiveBuffer{
		buf:       make([]byte, chunkSize),
		chunkSize: chunkSize,
	}
}

func (b *AdaptiveBuffer) Grow(size int) []byte {
	if size > len(b.buf) {
		b.reset(b.calculateNewSize(size))
	}

	b.usedBytes += size
	b.n++

	if b.averageUsage() < 0.25 {
		b.reset(b.calculateNewSize(int(0.25 * float64(len(b.buf)))))
	}
	return b.buf
}

func (b *AdaptiveBuffer) reset(size int) {
	b.buf = make([]byte, size)
	b.n = 0
	b.usedBytes = 0
}

func (b *AdaptiveBuffer) calculateNewSize(n int) int {
	return ((n + b.chunkSize - 1) / b.chunkSize) * b.chunkSize
}

func (b *AdaptiveBuffer) averageUsage() float64 {
	return float64(b.usedBytes) / float64((len(b.buf))*b.n)
}

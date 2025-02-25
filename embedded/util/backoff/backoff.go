package backoff

import (
	"errors"
	"fmt"
	"math"
	"math/rand/v2"
	"time"
)

var ErrBackoffStop = errors.New("backoff stop")

type Backoff struct {
	MinDelay   time.Duration
	MaxDelay   time.Duration
	MaxRetries int
}

func (b *Backoff) Retry(operation func(int) error) error {
	delay := b.MinDelay

	var err error
	for n := 0; n < b.maxRetries(); n++ {
		err = operation(n)
		if errors.Is(err, ErrBackoffStop) {
			return nil
		}
		if err == nil {
			return nil
		}

		time.Sleep(jitter(delay))

		delay = delay * 2
		if delay > b.MaxDelay {
			delay = b.MaxDelay
		}
	}
	return fmt.Errorf("%w: operation failed after %d retries", err, b.MaxRetries)
}

func (b *Backoff) maxRetries() int {
	if b.MaxRetries >= 0 {
		return b.MaxRetries
	}
	return math.MaxInt
}

func jitter(d time.Duration) time.Duration {
	jitterFactor := rand.Float64() * float64(d) * 0.5
	return d + time.Duration(jitterFactor)
}

package client

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/codenotary/immudb/pkg/api/schema"
)

type Factory func() (schema.ImmuServiceClient, error)

type pool struct {
	idle     []schema.ImmuServiceClient
	mutex    sync.Mutex
	factory  Factory
	maxPool  int
	done     chan struct{}
	shutdown bool
	total    int32
}

func NewPool(size int, f Factory) (*pool, error) {
	p := &pool{
		idle:    make([]schema.ImmuServiceClient, size),
		factory: f,
		maxPool: size,
		done:    make(chan struct{}),
	}

	for i := 0; i < size; i++ {
		c, err := f()
		if err != nil {
			return nil, fmt.Errorf("unable to create client, error %v", err)
		}
		p.idle[i] = c
		atomic.AddInt32(&p.total, 1)
	}
	return p, nil
}

func (p *pool) Get() (schema.ImmuServiceClient, error) {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	if len(p.idle) == 0 {
		return nil, errors.New("empty pool")
	}

	var cl schema.ImmuServiceClient
	num := len(p.idle)
	cl, p.idle[num-1] = p.idle[num-1], nil
	p.idle = p.idle[:num-1]

	atomic.AddInt32(&p.total, -1)
	return cl, nil
}

func (p *pool) Release(c schema.ImmuServiceClient) {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	if !p.IsShutdown() && len(p.idle) < p.maxPool {
		p.idle = append(p.idle, c)
		atomic.AddInt32(&p.total, 1)
		return
	}

	//c.CloseSession() // @TODO: Close session
}

func (p *pool) Close() {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	for _, cl := range p.idle {
		_ = cl
		//_, _ = cl.CloseSession(ctx) // @TODO: Close session
	}
	if !p.shutdown {
		close(p.done)
		p.shutdown = true
		atomic.StoreInt32(&p.total, 0)
		p.idle = nil
	}
}

func (p *pool) IsShutdown() bool {
	select {
	case <-p.done:
		return true
	default:
		return false
	}
}

func (p *pool) Size() int {
	return int(atomic.LoadInt32(&p.total))
}

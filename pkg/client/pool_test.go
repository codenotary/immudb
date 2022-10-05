package client

import (
	"testing"

	"github.com/codenotary/immudb/pkg/api/schema"
)

func TestPoolGetsPopulatedOnNewPoolCreation(t *testing.T) {
	size := 3
	p, err := NewPool(size, factory)
	if err != nil {
		t.Fatalf("unable to create pool, error %v", err)
	}

	if expected, got := size, p.Size(); expected != got {
		t.Fatalf("Expectation do not match, expected %d got %d", expected, got)
	}
}

func TestPoolGetReturnAClientAndReduceItsSize(t *testing.T) {
	size := 3
	p, err := NewPool(size, factory)
	if err != nil {
		t.Fatalf("unable to create pool, error %v", err)
	}

	c, err := p.Get()
	if err != nil {
		t.Fatalf("unable to get client from pool, error %v", err)
	}

	if c == nil {
		t.Fatal("nil client received from pool")
	}

	if expected, got := (size - 1), p.Size(); expected != got {
		t.Fatalf("Expectation do not match, expected %d got %d", expected, got)
	}
}

func TestPoolReleaseClientGetsBackToPoolAndRecoverItsSize(t *testing.T) {
	size := 3
	p, err := NewPool(size, factory)
	if err != nil {
		t.Fatalf("unable to create pool, error %v", err)
	}

	c, err := p.Get()
	if err != nil {
		t.Fatalf("unable to get client from pool, error %v", err)
	}

	if c == nil {
		t.Fatal("nil client received from pool")
	}
	p.Release(c)
	if expected, got := size, p.Size(); expected != got {
		t.Fatalf("Expectation do not match, expected %d got %d", expected, got)
	}
}

func TestPoolGetsClosedOnCloseInvocation(t *testing.T) {
	size := 3
	p, err := NewPool(size, factory)
	if err != nil {
		t.Fatalf("unable to create pool, error %v", err)
	}
	if p.IsShutdown() {
		t.Fatal("Pool not expected in shutdown state")
	}

	p.Close()

	if !p.IsShutdown() {
		t.Fatal("Pool expected in shutdown state")
	}
	if expected, got := 0, p.Size(); expected != got {
		t.Fatalf("Expectation do not match, expected %d got %d", expected, got)
	}
}

func TestPoolSupportMultipleCloseInvocations(t *testing.T) {
	size := 3
	p, err := NewPool(size, factory)
	if err != nil {
		t.Fatalf("unable to create pool, error %v", err)
	}

	p.Close()
	p.Close()
	p.Close()
	p.Close()

	if !p.IsShutdown() {
		t.Fatal("Pool expected in shutdown state")
	}
}

// ImmuServiceClientMock ...
type ImmuServiceClientMock struct {
	schema.ImmuServiceClient
}

func factory() (schema.ImmuServiceClient, error) {
	return &ImmuServiceClientMock{}, nil
}

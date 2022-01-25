package embed

import (
	"context"
	"fmt"
	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/codenotary/immudb/pkg/client"
	"github.com/golang/protobuf/proto"
	"sync"
)

type casCache struct {
	rw     sync.RWMutex
	client client.ImmuClient
}

// NewFileCache returns a new file cache
func NewCASCache(opts *client.Options) (*casCache, error) {
	c := client.NewClient().WithOptions(opts)
	err := c.OpenSession(context.TODO(), []byte(opts.Username), []byte(opts.Password), opts.Database)
	if err != nil {
		return nil, err
	}
	return &casCache{client: c}, nil
}

func (w *casCache) Get(serverUUID string, db string) (*schema.ImmutableState, error) {
	w.rw.RLock()
	defer w.rw.RUnlock()
	e, err := w.client.Get(context.TODO(), []byte(fmt.Sprintf("%s-%s", serverUUID, db)))
	if err != nil {
		return nil, err
	}
	state := &schema.ImmutableState{}
	err = proto.Unmarshal(e.Value, state)
	if err != nil {
		return nil, err
	}
	return state, nil
}

func (w *casCache) Set(serverUUID string, db string, state *schema.ImmutableState) error {
	w.rw.Lock()
	defer w.rw.Unlock()
	s, err := proto.Marshal(state)
	if err != nil {
		return err
	}
	_, err = w.client.Set(context.TODO(), []byte(fmt.Sprintf("%s-%s", serverUUID, db)), s)
	if err != nil {
		return err
	}
	return nil
}

func (w *casCache) Lock(serverUUID string) (err error) {
	return fmt.Errorf("not implemented")
}

func (w *casCache) Unlock() (err error) {
	return fmt.Errorf("not implemented")
}

package embed

import (
	"context"
	"fmt"
	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/codenotary/immudb/pkg/client"
	"github.com/codenotary/immudb/pkg/client/cache"
	"github.com/golang/protobuf/proto"
	"google.golang.org/grpc/status"
	"strings"
	"sync"
)

type immudbStateCache struct {
	rw     sync.RWMutex
	client client.ImmuClient
}

func NewImmudbStateCache(opts *client.Options) (*immudbStateCache, error) {
	if opts == nil {
		opts = client.DefaultOptions()
	}
	c := client.NewClient().WithOptions(opts)
	err := c.OpenSession(context.TODO(), []byte(opts.Username), []byte(opts.Password), opts.Database)
	if err != nil {
		return nil, err
	}
	return &immudbStateCache{client: c}, nil
}

func (w *immudbStateCache) Get(serverUUID string, db string) (*schema.ImmutableState, error) {
	w.rw.RLock()
	defer w.rw.RUnlock()
	e, err := w.client.VerifiedGet(context.TODO(), []byte(fmt.Sprintf("%s-%s", serverUUID, db)))
	if err != nil {
		if st, ok := status.FromError(err); ok {
			if strings.HasPrefix(st.Message(), "key not found") {
				return nil, cache.ErrPrevStateNotFound
			}
		}
		return nil, err
	}
	state := &schema.ImmutableState{}
	err = proto.Unmarshal(e.Value, state)
	if err != nil {
		return nil, err
	}
	return state, nil
}

func (w *immudbStateCache) Set(serverUUID string, db string, state *schema.ImmutableState) error {
	w.rw.Lock()
	defer w.rw.Unlock()
	s, err := proto.Marshal(state)
	if err != nil {
		return err
	}
	_, err = w.client.VerifiedSet(context.TODO(), []byte(fmt.Sprintf("%s-%s", serverUUID, db)), s)
	if err != nil {
		return err
	}
	return nil
}

func (w *immudbStateCache) Lock(serverUUID string) (err error) {
	return fmt.Errorf("not implemented")
}

func (w *immudbStateCache) Unlock() (err error) {
	return fmt.Errorf("not implemented")
}

/*
Copyright 2022 Codenotary Inc. All rights reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

	http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package client

import (
	"context"
	"net"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/codenotary/immudb/pkg/client/heartbeater"
	"github.com/codenotary/immudb/pkg/stream"
	"github.com/golang/protobuf/ptypes/empty"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

func TestImmuClient_OpenSession_ErrParsingKey(t *testing.T) {
	c := NewClient().WithOptions(DefaultOptions().WithServerSigningPubKey("invalid"))
	err := c.OpenSession(context.TODO(), []byte(`immudb`), []byte(`immudb`), "defaultdb")
	require.Error(t, err)
	require.ErrorIs(t, err, syscall.ENOENT)
}

func TestImmuClient_OpenSession_ErrDefaultChunkTooSmall(t *testing.T) {
	c := NewClient().WithOptions(DefaultOptions().WithStreamChunkSize(1))
	err := c.OpenSession(context.TODO(), []byte(`immudb`), []byte(`immudb`), "defaultdb")
	require.Error(t, err)
	require.Equal(t, err.Error(), stream.ErrChunkTooSmall)
}

func TestImmuClient_OpenSession_DialError(t *testing.T) {
	c := NewClient().WithOptions(DefaultOptions().WithDialOptions([]grpc.DialOption{grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
		return nil, syscall.ECONNREFUSED
	})}))
	err := c.OpenSession(context.TODO(), []byte(`immudb`), []byte(`immudb`), "defaultdb")
	require.Error(t, err)
}

func TestImmuClient_OpenSession_OpenSessionError(t *testing.T) {
	c := NewClient()
	err := c.OpenSession(context.TODO(), nil, nil, "")
	require.Error(t, err)
}

func TestImmuClient_OpenSession_OpenAndCloseSessionAfterError_AvoidPanic(t *testing.T) {
	c := NewClient()
	err := c.OpenSession(context.TODO(), nil, nil, "")
	require.Error(t, err)
	// try open session again
	err = c.OpenSession(context.TODO(), nil, nil, "")
	require.NotErrorIs(t, err, ErrSessionAlreadyOpen)
	// close over not open session
	err = c.CloseSession(context.TODO())
	require.NotErrorIs(t, err, ErrSessionAlreadyOpen)
}

func TestImmuClient_OpenSession_StateServiceError(t *testing.T) {
	c := NewClient().WithOptions(DefaultOptions().WithDir("false"))
	c.ServiceClient = &immuServiceClientMock{
		OpenSessionF: func(ctx context.Context, in *schema.OpenSessionRequest, opts ...grpc.CallOption) (*schema.OpenSessionResponse, error) {
			return &schema.OpenSessionResponse{
				SessionID: "test",
			}, nil
		},
		KeepAliveF: func(ctx context.Context, in *empty.Empty, opts ...grpc.CallOption) (*empty.Empty, error) {
			return new(empty.Empty), nil
		},
	}
	err := c.OpenSession(context.TODO(), []byte(`immudb`), []byte(`immudb`), "defaultdb")
	require.Error(t, err)
}

func TestImmuClient_OpenSession_DoesNotPropagateContextCancelationToHeartBeaterKeepAlives(t *testing.T) {
	c := NewClient().WithOptions(DefaultOptions().WithDir("false"))
	var receivedContext context.Context
	wg := sync.WaitGroup{}
	wg.Add(1)

	mu := sync.Mutex{}

	serviceClient := &immuServiceClientMock{
		OpenSessionF: func(ctx context.Context, in *schema.OpenSessionRequest, opts ...grpc.CallOption) (*schema.OpenSessionResponse, error) {
			return &schema.OpenSessionResponse{
				SessionID: "test",
			}, nil
		},
		KeepAliveF: func(ctx context.Context, in *empty.Empty, opts ...grpc.CallOption) (*empty.Empty, error) {
			mu.Lock()
			defer mu.Unlock()
			receivedContext = ctx
			wg.Done()
			return new(empty.Empty), nil
		},
	}
	c.ServiceClient = serviceClient

	mu.Lock()
	ctx, cancel := context.WithCancel(context.Background())
	err := c.OpenSession(ctx, []byte(`immudb`), []byte(`immudb`), "defaultdb") // @TODO: Open Session replaces original Service Client indeed
	require.NoError(t, err)
	c.ServiceClient = serviceClient // @TODO: Race condition, it's used from HeartBeater goroutine
	mu.Unlock()
	c.HeartBeater.Stop()

	c.Options.HeartBeatFrequency = time.Second * 2
	c.HeartBeater = heartbeater.NewHeartBeater(c.SessionID, serviceClient, c.Options.HeartBeatFrequency)
	c.HeartBeater.KeepAlive(context.Background()) // @TODO: Needs to be cancelled before moving ahead

	cancel()

	// wait until KeepAliveF has been called
	done := make(chan struct{})
	go func() {
		defer close(done)
		wg.Wait()
	}()
	select {
	case <-done:
	case <-time.After(time.Second * 5):
		t.Fatal("timeout waiting completion")
	}

	if receivedContext == nil {
		t.Fatal("received context is nil")
	}

	select {
	case <-receivedContext.Done(): // @TODO: Context with 3 seconds timeout scope
		t.Fatal("parent context canceled")
	case <-ctx.Done():
	}
}

type immuServiceClientMock struct {
	schema.ImmuServiceClient
	OpenSessionF func(ctx context.Context, in *schema.OpenSessionRequest, opts ...grpc.CallOption) (*schema.OpenSessionResponse, error)
	KeepAliveF   func(ctx context.Context, in *empty.Empty, opts ...grpc.CallOption) (*empty.Empty, error)
}

func (icm *immuServiceClientMock) OpenSession(ctx context.Context, in *schema.OpenSessionRequest, opts ...grpc.CallOption) (*schema.OpenSessionResponse, error) {
	return icm.OpenSessionF(ctx, in, opts...)
}
func (icm *immuServiceClientMock) KeepAlive(ctx context.Context, in *empty.Empty, opts ...grpc.CallOption) (*empty.Empty, error) {
	return icm.KeepAliveF(ctx, in, opts...)
}

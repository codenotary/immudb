/*
Copyright 2025 Codenotary Inc. All rights reserved.

SPDX-License-Identifier: BUSL-1.1
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    https://mariadb.com/bsl11/

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package client

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/golang/protobuf/ptypes/empty"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

type mockImmuServiceClient struct {
	schema.ImmuServiceClient
	keepAliveErr error
	keepAliveCh  chan struct{}
}

func (m *mockImmuServiceClient) KeepAlive(ctx context.Context, in *empty.Empty, opts ...grpc.CallOption) (*empty.Empty, error) {
	if m.keepAliveCh != nil {
		m.keepAliveCh <- struct{}{}
	}
	return &empty.Empty{}, m.keepAliveErr
}

func TestNewHeartBeater(t *testing.T) {
	sc := &mockImmuServiceClient{}
	hb := NewHeartBeater("test-session", sc, time.Second, nil, nil)

	require.NotNil(t, hb)
	require.Equal(t, "test-session", hb.sessionID)
	require.NotNil(t, hb.logger)
	require.NotNil(t, hb.done)
	require.NotNil(t, hb.t)
}

func TestHeartBeaterKeepAliveAndStop(t *testing.T) {
	called := make(chan struct{}, 5)
	sc := &mockImmuServiceClient{keepAliveCh: called}
	hb := NewHeartBeater("test-session", sc, 50*time.Millisecond, nil, nil)

	hb.KeepAlive(context.Background())

	// Wait for at least one keep-alive call
	select {
	case <-called:
	case <-time.After(time.Second):
		t.Fatal("keep alive was not called")
	}

	hb.Stop()
}

func TestHeartBeaterKeepAliveError(t *testing.T) {
	errCh := make(chan error, 1)
	sc := &mockImmuServiceClient{
		keepAliveErr: fmt.Errorf("connection lost"),
		keepAliveCh:  make(chan struct{}, 5),
	}
	handler := func(sessionID string, err error) {
		errCh <- err
	}
	hb := NewHeartBeater("test-session", sc, 50*time.Millisecond, handler, nil)

	hb.KeepAlive(context.Background())

	select {
	case err := <-errCh:
		require.EqualError(t, err, "connection lost")
	case <-time.After(time.Second):
		t.Fatal("error handler was not called")
	}

	hb.Stop()
}

func TestHeartBeaterKeepAliveRequest(t *testing.T) {
	sc := &mockImmuServiceClient{}
	hb := NewHeartBeater("test-session", sc, time.Second, nil, nil)

	err := hb.keepAliveRequest(context.Background())
	require.NoError(t, err)

	sc.keepAliveErr = fmt.Errorf("rpc error")
	err = hb.keepAliveRequest(context.Background())
	require.EqualError(t, err, "rpc error")
}

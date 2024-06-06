/*
Copyright 2024 Codenotary Inc. All rights reserved.

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
	"net"
	"syscall"
	"testing"

	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/codenotary/immudb/pkg/stream"
	"github.com/golang/protobuf/ptypes/empty"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

func TestImmuClient_OpenSession_ErrParsingKey(t *testing.T) {
	c := NewClient().WithOptions(DefaultOptions().WithServerSigningPubKey("invalid"))
	err := c.OpenSession(context.Background(), []byte(`immudb`), []byte(`immudb`), "defaultdb")
	require.ErrorIs(t, err, syscall.ENOENT)
}

func TestImmuClient_OpenSession_ErrDefaultChunkTooSmall(t *testing.T) {
	c := NewClient().WithOptions(DefaultOptions().WithStreamChunkSize(1))
	err := c.OpenSession(context.Background(), []byte(`immudb`), []byte(`immudb`), "defaultdb")
	require.ErrorContains(t, err, stream.ErrChunkTooSmall)
}

func TestImmuClient_OpenSession_DialError(t *testing.T) {
	c := NewClient().WithOptions(DefaultOptions().WithDialOptions([]grpc.DialOption{grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
		return nil, syscall.ECONNREFUSED
	})}))
	err := c.OpenSession(context.Background(), []byte(`immudb`), []byte(`immudb`), "defaultdb")
	require.Error(t, err)
}

func TestImmuClient_OpenSession_OpenSessionError(t *testing.T) {
	c := NewClient()
	err := c.OpenSession(context.Background(), nil, nil, "")
	require.Error(t, err)
}

func TestImmuClient_OpenSession_OpenAndCloseSessionAfterError_AvoidPanic(t *testing.T) {
	c := NewClient()
	err := c.OpenSession(context.Background(), nil, nil, "")
	require.Error(t, err)
	// try open session again
	err = c.OpenSession(context.Background(), nil, nil, "")
	require.NotErrorIs(t, err, ErrSessionAlreadyOpen)
	// close over not open session
	err = c.CloseSession(context.Background())
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
	err := c.OpenSession(context.Background(), []byte(`immudb`), []byte(`immudb`), "defaultdb")
	require.Error(t, err)
}

type immuServiceClientMock struct {
	schema.ImmuServiceClient
	OpenSessionF func(ctx context.Context, in *schema.OpenSessionRequest, opts ...grpc.CallOption) (*schema.OpenSessionResponse, error)
	KeepAliveF   func(ctx context.Context, in *empty.Empty, opts ...grpc.CallOption) (*empty.Empty, error)
	TruncateF    func(ctx context.Context, in *schema.TruncateDatabaseRequest, opts ...grpc.CallOption) (*schema.TruncateDatabaseResponse, error)
}

func (icm *immuServiceClientMock) OpenSession(ctx context.Context, in *schema.OpenSessionRequest, opts ...grpc.CallOption) (*schema.OpenSessionResponse, error) {
	return icm.OpenSessionF(ctx, in, opts...)
}

func (icm *immuServiceClientMock) KeepAlive(ctx context.Context, in *empty.Empty, opts ...grpc.CallOption) (*empty.Empty, error) {
	return icm.KeepAliveF(ctx, in, opts...)
}

func (icm *immuServiceClientMock) TruncateDatabase(ctx context.Context, in *schema.TruncateDatabaseRequest, opts ...grpc.CallOption) (*schema.TruncateDatabaseResponse, error) {
	return icm.TruncateF(ctx, in, opts...)
}

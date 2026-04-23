/*
Copyright 2026 Codenotary Inc. All rights reserved.

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
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
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
	// A real gRPC server is required here because OpenSession creates its own
	// service client from the dial connection; setting c.ServiceClient has no
	// effect on that internal call.  The server returns a session with an empty
	// ServerUUID, which causes NewStateServiceWithUUID to fail with ErrNoServerUuid.
	lis := bufconn.Listen(1 << 20)
	srv := grpc.NewServer()
	schema.RegisterImmuServiceServer(srv, emptyUUIDImmuServer{})
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)

	c := NewClient().WithOptions(DefaultOptions().
		WithDir("false").
		WithDialOptions([]grpc.DialOption{
			grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
				return lis.DialContext(ctx)
			}),
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		}))

	err := c.OpenSession(context.Background(), []byte(`immudb`), []byte(`immudb`), "defaultdb")
	require.Error(t, err)
}

// emptyUUIDImmuServer accepts OpenSession and returns a session ID with no
// ServerUUID, causing NewStateServiceWithUUID to return ErrNoServerUuid.
type emptyUUIDImmuServer struct{ schema.UnimplementedImmuServiceServer }

func (emptyUUIDImmuServer) OpenSession(_ context.Context, _ *schema.OpenSessionRequest) (*schema.OpenSessionResponse, error) {
	return &schema.OpenSessionResponse{SessionID: "test-session"}, nil
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

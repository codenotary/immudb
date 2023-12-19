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

package server

import (
	"context"
	"errors"
	"testing"

	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/codenotary/immudb/pkg/auth"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

func TestExportTxEdgeCases(t *testing.T) {
	dir := t.TempDir()

	serverOptions := DefaultOptions().
		WithDir(dir).
		WithMetricsServer(false).
		WithAdminPassword(auth.SysAdminPassword)

	s := DefaultServer().WithOptions(serverOptions).(*ImmuServer)

	s.Initialize()

	err := s.ExportTx(nil, nil)
	require.ErrorIs(t, err, ErrIllegalArguments)

	err = s.ExportTx(&schema.ExportTxRequest{Tx: 1}, &immuServiceExportTxServer{})
	require.ErrorIs(t, err, ErrNotLoggedIn)

	ctx := context.Background()

	lr, err := s.Login(ctx, &schema.LoginRequest{
		User:     []byte(auth.SysAdminUsername),
		Password: []byte(auth.SysAdminPassword),
	})
	require.NoError(t, err)

	md := metadata.Pairs("authorization", lr.Token)
	ctx = metadata.NewIncomingContext(context.Background(), md)

	err = s.ExportTx(&schema.ExportTxRequest{Tx: 0}, &immuServiceExportTxServer{})
	require.ErrorIs(t, err, ErrIllegalArguments)

	err = s.ExportTx(&schema.ExportTxRequest{Tx: 1}, &immuServiceExportTxServer{})
	require.ErrorIs(t, err, ErrNotLoggedIn)
}

func TestReplicateTxEdgeCases(t *testing.T) {
	dir := t.TempDir()

	serverOptions := DefaultOptions().
		WithDir(dir).
		WithMetricsServer(false).
		WithAdminPassword(auth.SysAdminPassword)

	s := DefaultServer().WithOptions(serverOptions).(*ImmuServer)

	s.Initialize()

	err := s.ReplicateTx(nil)
	require.ErrorIs(t, err, ErrIllegalArguments)

	err = s.ReplicateTx(&immuServiceReplicateTxServer{ctx: context.Background()})
	require.ErrorIs(t, err, ErrNotLoggedIn)

	ctx := context.Background()

	lr, err := s.Login(ctx, &schema.LoginRequest{
		User:     []byte(auth.SysAdminUsername),
		Password: []byte(auth.SysAdminPassword),
	})
	require.NoError(t, err)

	md := metadata.Pairs("authorization", lr.Token)
	ctx = metadata.NewIncomingContext(context.Background(), md)

	stream := &immuServiceReplicateTxServer{ctx: ctx}

	err = s.ReplicateTx(stream)
	require.ErrorContains(t, err, "error")
}

type immuServiceExportTxServer struct {
	grpc.ServerStream
}

func (s *immuServiceExportTxServer) Send(chunk *schema.Chunk) error {
	return errors.New("error")
}

func (s *immuServiceExportTxServer) SendAndClose(tx *schema.VerifiableTx) error {
	return nil
}

func (s *immuServiceExportTxServer) Recv() (*schema.Chunk, error) {
	return nil, nil
}

func (s *immuServiceExportTxServer) Context() context.Context {
	return context.Background()
}

type immuServiceReplicateTxServer struct {
	grpc.ServerStream
	ctx context.Context
}

func (s *immuServiceReplicateTxServer) Recv() (*schema.Chunk, error) {
	return nil, errors.New("error")
}

func (s *immuServiceReplicateTxServer) SendAndClose(md *schema.TxHeader) error {
	return nil
}

func (s *immuServiceReplicateTxServer) Context() context.Context {
	return s.ctx
}

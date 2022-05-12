/*
Copyright 2022 CodeNotary, Inc. All rights reserved.

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

package server

import (
	"context"
	"errors"
	"os"
	"testing"

	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/codenotary/immudb/pkg/auth"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

func TestExportTxEdgeCases(t *testing.T) {
	serverOptions := DefaultOptions().WithMetricsServer(false).WithAdminPassword(auth.SysAdminPassword)
	s := DefaultServer().WithOptions(serverOptions).(*ImmuServer)
	defer os.RemoveAll(s.Options.Dir)

	s.Initialize()

	err := s.ExportTx(nil, nil)
	require.Equal(t, ErrIllegalArguments, err)

	err = s.ExportTx(&schema.ExportTxRequest{Tx: 1}, &immuServiceExportTxServer{})
	require.Error(t, err)

	ctx := context.Background()

	lr, err := s.Login(ctx, &schema.LoginRequest{
		User:     []byte(auth.SysAdminUsername),
		Password: []byte(auth.SysAdminPassword),
	})
	require.NoError(t, err)

	md := metadata.Pairs("authorization", lr.Token)
	ctx = metadata.NewIncomingContext(context.Background(), md)

	err = s.ExportTx(&schema.ExportTxRequest{Tx: 0}, &immuServiceExportTxServer{})
	require.Equal(t, ErrIllegalArguments, err)

	err = s.ExportTx(&schema.ExportTxRequest{Tx: 1}, &immuServiceExportTxServer{})
	require.Error(t, err)
}

func TestReplicateTxEdgeCases(t *testing.T) {
	serverOptions := DefaultOptions().WithMetricsServer(false).WithAdminPassword(auth.SysAdminPassword)
	s := DefaultServer().WithOptions(serverOptions).(*ImmuServer)
	defer os.RemoveAll(s.Options.Dir)

	s.Initialize()

	err := s.ReplicateTx(nil)
	require.Equal(t, ErrIllegalArguments, err)

	err = s.ReplicateTx(&immuServiceReplicateTxServer{ctx: context.Background()})
	require.Error(t, err)

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
	require.Error(t, err)
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
	return context.TODO()
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

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

package server

import (
	"context"
	"testing"

	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

func TestImmuServer_StreamGetDbError(t *testing.T) {
	dir := t.TempDir()

	s := DefaultServer()

	s.WithOptions(DefaultOptions().WithDir(dir))

	err := s.StreamSet(&StreamServerMock{})
	require.Error(t, err)
	err = s.StreamGet(nil, &StreamServerMock{})
	require.Error(t, err)
	err = s.StreamScan(nil, &StreamServerMock{})
	require.Error(t, err)
	err = s.StreamHistory(nil, &StreamServerMock{})
	require.Error(t, err)
	err = s.StreamVerifiableGet(nil, &StreamVerifiableServerMock{})
	require.Error(t, err)
	err = s.StreamVerifiableSet(&StreamVerifiableServerMock{})
	require.Error(t, err)
	err = s.StreamZScan(nil, &StreamServerMock{})
	require.Error(t, err)
	err = s.StreamExecAll(&StreamServerMock{})
	require.Error(t, err)
}

type StreamServerMock struct {
	grpc.ServerStream
}

func (s *StreamServerMock) Send(chunk *schema.Chunk) error {
	return nil
}

func (s *StreamServerMock) SendAndClose(*schema.TxHeader) error {
	return nil
}
func (s *StreamServerMock) Recv() (*schema.Chunk, error) {
	return nil, nil
}
func (s *StreamServerMock) Context() context.Context {
	return context.Background()
}

type StreamVerifiableServerMock struct {
	grpc.ServerStream
}

func (s *StreamVerifiableServerMock) Send(chunk *schema.Chunk) error {
	return nil
}

func (s *StreamVerifiableServerMock) SendAndClose(tx *schema.VerifiableTx) error {
	return nil
}
func (s *StreamVerifiableServerMock) Recv() (*schema.Chunk, error) {
	return nil, nil
}
func (s *StreamVerifiableServerMock) Context() context.Context {
	return context.Background()
}

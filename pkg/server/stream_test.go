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
	require.ErrorIs(t, err, ErrNotLoggedIn)
	err = s.StreamGet(nil, &StreamServerMock{})
	require.ErrorIs(t, err, ErrNotLoggedIn)
	err = s.StreamScan(nil, &StreamServerMock{})
	require.ErrorIs(t, err, ErrNotLoggedIn)
	err = s.StreamHistory(nil, &StreamServerMock{})
	require.ErrorIs(t, err, ErrNotLoggedIn)
	err = s.StreamVerifiableGet(nil, &StreamVerifiableServerMock{})
	require.ErrorIs(t, err, ErrNotLoggedIn)
	err = s.StreamVerifiableSet(&StreamVerifiableServerMock{})
	require.ErrorIs(t, err, ErrNotLoggedIn)
	err = s.StreamZScan(nil, &StreamServerMock{})
	require.ErrorIs(t, err, ErrNotLoggedIn)
	err = s.StreamExecAll(&StreamServerMock{})
	require.ErrorIs(t, err, ErrNotLoggedIn)
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

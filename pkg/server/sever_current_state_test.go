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

package server

import (
	"context"
	"testing"

	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/codenotary/immudb/pkg/signer"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/emptypb"
)

func TestServerCurrentStateSigned(t *testing.T) {
	dir := t.TempDir()

	s := DefaultServer()

	s.WithOptions(DefaultOptions().WithDir(dir))

	dbRootpath := dir

	sig, err := signer.NewSigner("./../../test/signer/ec3.key")
	require.NoError(t, err)

	stSig := NewStateSigner(sig)
	s = s.WithOptions(s.Options.WithAuth(false).WithSigningKey("foo")).WithStateSigner(stSig).(*ImmuServer)

	err = s.loadSystemDatabase(dbRootpath, nil, s.Options.AdminPassword, false)
	require.NoError(t, err)

	err = s.loadDefaultDatabase(dbRootpath, nil)
	require.NoError(t, err)

	ctx := context.Background()

	_, _ = s.Set(ctx, &schema.SetRequest{
		KVs: []*schema.KeyValue{
			{
				Key:   []byte("Alberto"),
				Value: []byte("Tomba"),
			},
		},
	},
	)

	state, err := s.CurrentState(ctx, &emptypb.Empty{})

	require.NoError(t, err)
	require.IsType(t, &schema.ImmutableState{}, state)
	require.IsType(t, &schema.Signature{}, state.Signature)
	require.NotNil(t, state.Signature.Signature)
	require.NotNil(t, state.Signature.PublicKey)

	ecdsaPK, err := signer.UnmarshalKey(state.Signature.PublicKey)
	require.NoError(t, err)

	err = signer.Verify(state.ToBytes(), state.Signature.Signature, ecdsaPK)
	require.NoError(t, err)
}

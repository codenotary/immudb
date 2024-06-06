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
	"testing"

	"github.com/codenotary/immudb/embedded/store"
	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/codenotary/immudb/pkg/signer"
	"github.com/stretchr/testify/require"
)

func TestNewStateSigner(t *testing.T) {
	s, _ := signer.NewSigner("./../../test/signer/ec3.key")
	rs := NewStateSigner(s)
	require.IsType(t, &stateSigner{}, rs)
}

func TestStateSigner_Sign(t *testing.T) {
	s, _ := signer.NewSigner("./../../test/signer/ec3.key")
	stSigner := NewStateSigner(s)
	state := &schema.ImmutableState{}
	err := stSigner.Sign(state)
	require.NoError(t, err)
	require.IsType(t, &schema.ImmutableState{}, state)
}

func TestStateSigner_Err(t *testing.T) {
	s, _ := signer.NewSigner("./../../test/signer/ec3.key")
	stSigner := NewStateSigner(s)
	err := stSigner.Sign(nil)
	require.ErrorIs(t, err, store.ErrIllegalArguments)
}

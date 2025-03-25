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
	"github.com/codenotary/immudb/embedded/store"
	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/codenotary/immudb/pkg/signer"
)

type StateSigner interface {
	Sign(state *schema.ImmutableState) error
}

type stateSigner struct {
	Signer signer.Signer
}

func NewStateSigner(signer signer.Signer) *stateSigner {
	return &stateSigner{
		Signer: signer,
	}
}

func (sts *stateSigner) Sign(state *schema.ImmutableState) error {
	if state == nil {
		return store.ErrIllegalArguments
	}

	signature, publicKey, err := sts.Signer.Sign(state.ToBytes())

	state.Signature = &schema.Signature{
		Signature: signature,
		PublicKey: publicKey,
	}

	return err
}

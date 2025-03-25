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

package stream

import (
	"io"

	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/golang/protobuf/proto"
)

// ParseVerifiableEntry ...
func ParseVerifiableEntry(
	entryWithoutValueProto []byte,
	verifiableTxProto []byte,
	inclusionProofProto []byte,
	vr io.Reader,
	chunkSize int,
) (*schema.VerifiableEntry, error) {

	var entry schema.Entry
	if err := proto.Unmarshal(entryWithoutValueProto, &entry); err != nil {
		return nil, err
	}

	var verifiableTx schema.VerifiableTx
	if err := proto.Unmarshal(verifiableTxProto, &verifiableTx); err != nil {
		return nil, err
	}

	var inclusionProof schema.InclusionProof
	if err := proto.Unmarshal(inclusionProofProto, &inclusionProof); err != nil {
		return nil, err
	}

	value, err := ReadValue(vr, chunkSize)
	if err != nil {
		return nil, err
	}
	// set the value on the entry, as it came without it
	entry.Value = value

	return &schema.VerifiableEntry{
		Entry:          &entry,
		VerifiableTx:   &verifiableTx,
		InclusionProof: &inclusionProof,
	}, nil
}

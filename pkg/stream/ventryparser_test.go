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
	"bufio"
	"bytes"
	"io"
	"testing"

	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/require"
)

func TestParseVerifiableEntryErrors(t *testing.T) {
	_, err := ParseVerifiableEntry([]byte("not a proto message"), nil, nil, nil, 0)
	require.ErrorContains(t, err, "cannot parse invalid wire-format data")

	entryWithoutValueBs, err := proto.Marshal(&schema.Entry{})
	require.NoError(t, err)
	_, err = ParseVerifiableEntry(
		entryWithoutValueBs, []byte("not a proto message"), nil, nil, 0)
	require.ErrorContains(t, err, "cannot parse invalid wire-format data")

	verifiableTxBs, err := proto.Marshal(&schema.VerifiableTx{})
	require.NoError(t, err)
	_, err = ParseVerifiableEntry(
		entryWithoutValueBs, verifiableTxBs, []byte("not a proto message"), nil, 0)
	require.ErrorContains(t, err, "cannot parse invalid wire-format data")

	inclusionProofBs, err := proto.Marshal(&schema.InclusionProof{})
	require.NoError(t, err)
	valueReader := bufio.NewReader(bytes.NewBuffer([]byte{}))
	_, err = ParseVerifiableEntry(
		entryWithoutValueBs, verifiableTxBs, inclusionProofBs, valueReader, 0)
	require.ErrorIs(t, err, io.EOF)

	valueReader = bufio.NewReader(bytes.NewBuffer([]byte("some value")))
	_, err = ParseVerifiableEntry(
		entryWithoutValueBs, verifiableTxBs, inclusionProofBs, valueReader, MinChunkSize)
	require.NoError(t, err)
}

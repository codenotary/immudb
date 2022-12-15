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

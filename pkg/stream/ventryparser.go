package stream

import (
	"bufio"

	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/golang/protobuf/proto"
)

// ParseVerifiableEntry ...
func ParseVerifiableEntry(
	entryWithoutValueProto []byte,
	verifiableTxProto []byte,
	inclusionProofProto []byte,
	vr *bufio.Reader,
	chunkSize int,
) (*schema.VerifiableEntry, error) {

	var entry schema.Entry
	if err := proto.Unmarshal(verifiableTxProto, &entry); err != nil {
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

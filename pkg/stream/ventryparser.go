package stream

import (
	"bufio"

	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/golang/protobuf/proto"
)

// ParseVerifiableEntry ...
func ParseVerifiableEntry(
	key []byte,
	verifiableTxBs []byte,
	inclusionProofBs []byte,
	vr *bufio.Reader,
	chunkSize int,
) (*schema.VerifiableEntry, error) {

	var verifiableTx schema.VerifiableTx
	if err := proto.Unmarshal(verifiableTxBs, &verifiableTx); err != nil {
		return nil, err
	}

	var inclusionProof schema.InclusionProof
	if err := proto.Unmarshal(inclusionProofBs, &inclusionProof); err != nil {
		return nil, err
	}

	value, err := ParseValue(vr, chunkSize)
	if err != nil {
		return nil, err
	}

	entry := schema.Entry{Key: key, Value: value}

	return &schema.VerifiableEntry{
		Entry:          &entry,
		VerifiableTx:   &verifiableTx,
		InclusionProof: &inclusionProof,
	}, nil
}

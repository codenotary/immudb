/*
Copyright 2021 CodeNotary, Inc. All rights reserved.

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
package store

import (
	"bytes"
	"encoding/binary"
)

const maxTxMetadataLen = sszSize + maxTxMetadataSummaryLen
const maxTxMetadataSummaryLen = 256

type TxMetadata struct {
	summary []byte
}

func NewTxMetadata() *TxMetadata {
	return &TxMetadata{}
}

func (md *TxMetadata) WithSummary(summary []byte) *TxMetadata {
	md.summary = make([]byte, len(summary))
	copy(md.summary, summary)
	return md
}

func (md *TxMetadata) Equal(amd *TxMetadata) bool {
	if amd == nil {
		return false
	}

	return bytes.Equal(md.summary, amd.summary)
}

func (md *TxMetadata) Bytes() []byte {
	b := make([]byte, sszSize+len(md.summary))

	binary.BigEndian.PutUint16(b, uint16(len(md.summary)))
	copy(b[sszSize:], md.summary)

	return b
}

func (md *TxMetadata) Summary() []byte {
	return md.summary
}

func (md *TxMetadata) ReadFrom(b []byte) error {
	if len(b) == 0 {
		return nil
	}

	if len(b) < sszSize {
		return ErrCorruptedData
	}

	slen := int(binary.BigEndian.Uint16(b))

	if len(b) < sszSize+slen || slen > maxTxMetadataSummaryLen {
		return ErrCorruptedData
	}

	md.summary = make([]byte, slen)
	copy(md.summary, b[sszSize:sszSize+slen])

	return nil
}

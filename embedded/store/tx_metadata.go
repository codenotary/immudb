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

package store

import (
	"bytes"
	"encoding/binary"
	"fmt"
)

// attributeCode is used to identify the attribute.
const (
	truncatedUptoTxAttrCode attributeCode = 0
	extraAttrCode           attributeCode = 1
)

// attribute size is the size of the attribute in bytes.
const (
	truncatedUptoTxAttrSize = txIDSize
)

const maxTxMetadataLen = (attrCodeSize + truncatedUptoTxAttrSize) +
	(attrCodeSize + sszSize + maxExtraLen)

const maxExtraLen = 256

// truncatedUptoTxAttribute is used to identify that the transaction
// stores the information up to which given transaction ID the
// database was truncated.
type truncatedUptoTxAttribute struct {
	txID uint64
}

// code returns the attribute code.
func (a *truncatedUptoTxAttribute) code() attributeCode {
	return truncatedUptoTxAttrCode
}

// serialize returns the serialized attribute.
func (a *truncatedUptoTxAttribute) serialize() []byte {
	var b [txIDSize]byte
	binary.BigEndian.PutUint64(b[:], a.txID)
	return b[:]
}

// deserialize deserializes the attribute.
func (a *truncatedUptoTxAttribute) deserialize(b []byte) (int, error) {
	if len(b) < txIDSize {
		return 0, ErrCorruptedData
	}

	a.txID = binary.BigEndian.Uint64(b)
	return txIDSize, nil
}

type extraAttribute struct {
	extra []byte
}

// code returns the attribute code.
func (a *extraAttribute) code() attributeCode {
	return extraAttrCode
}

// serialize returns the serialized attribute.
func (a *extraAttribute) serialize() []byte {
	var b [sszSize + maxExtraLen]byte

	binary.BigEndian.PutUint16(b[:], uint16(len(a.extra)))
	copy(b[sszSize:], a.extra)

	return b[:sszSize+len(a.extra)]
}

// deserialize deserializes the attribute.
func (a *extraAttribute) deserialize(b []byte) (int, error) {
	if len(b) < sszSize {
		return 0, ErrCorruptedData
	}

	a.extra = make([]byte, binary.BigEndian.Uint16(b))
	copy(a.extra, b[sszSize:])

	return sszSize + len(a.extra), nil
}

func getAttributeFrom(attrCode attributeCode) (attribute, error) {
	switch attrCode {
	case truncatedUptoTxAttrCode:
		{
			return &truncatedUptoTxAttribute{}, nil
		}
	case extraAttrCode:
		{
			return &extraAttribute{}, nil
		}
	default:
		{
			return nil, fmt.Errorf("error reading tx metadata attributes: %w", ErrCorruptedData)
		}
	}
}

// TxMetadata is used to store metadata of a transaction.
type TxMetadata struct {
	// attributes is a map of attributes.
	attributes map[attributeCode]attribute
}

func NewTxMetadata() *TxMetadata {
	return &TxMetadata{
		attributes: make(map[attributeCode]attribute),
	}
}

func (md *TxMetadata) IsEmpty() bool {
	return md == nil || len(md.attributes) == 0
}

func (md *TxMetadata) HasExtraOnly() bool {
	return len(md.attributes) == 1 && md.Extra() != nil
}

func (md *TxMetadata) Equal(amd *TxMetadata) bool {
	if amd == nil || md == nil {
		return false
	}
	return bytes.Equal(md.Bytes(), amd.Bytes())
}

func (md *TxMetadata) Bytes() []byte {
	var b bytes.Buffer

	for _, attrCode := range []attributeCode{truncatedUptoTxAttrCode, extraAttrCode} {
		attr, ok := md.attributes[attrCode]
		if ok {
			b.WriteByte(byte(attr.code()))
			b.Write(attr.serialize())
		}
	}

	return b.Bytes()
}

func (md *TxMetadata) ReadFrom(b []byte) error {
	if len(b) > maxTxMetadataLen {
		return ErrCorruptedData
	}

	i := 0

	for {
		if len(b) == i {
			break
		}

		if len(b[i:]) < attrCodeSize {
			return ErrCorruptedData
		}

		attrCode := attributeCode(b[i])
		i += attrCodeSize

		attr, err := getAttributeFrom(attrCode)
		if err != nil {
			return err
		}

		n, err := attr.deserialize(b[i:])
		if err != nil {
			return fmt.Errorf("error reading tx metadata attributes: %w", err)
		}
		i += n

		md.attributes[attr.code()] = attr
	}

	return nil
}

// HasTruncatedTxID returns true if the transaction stores the information
// up to which given transaction ID the database was truncated.
func (md *TxMetadata) HasTruncatedTxID() bool {
	_, ok := md.attributes[truncatedUptoTxAttrCode]
	return ok
}

// GetTruncatedTxID returns the transaction ID up to which the
// database was last truncated.
func (md *TxMetadata) GetTruncatedTxID() (uint64, error) {
	attr, ok := md.attributes[truncatedUptoTxAttrCode]
	if !ok {
		return 0, ErrTruncationInfoNotPresentInMetadata
	}

	return attr.(*truncatedUptoTxAttribute).txID, nil
}

// WithTruncatedTxID sets the vlog truncated attribute indicating
// that the transaction stores the information up to which given
// transaction ID the database was truncated.
func (md *TxMetadata) WithTruncatedTxID(txID uint64) *TxMetadata {
	attr, ok := md.attributes[truncatedUptoTxAttrCode]
	if !ok {
		attr = &truncatedUptoTxAttribute{txID: txID}
		md.attributes[truncatedUptoTxAttrCode] = attr
		return md
	}

	attr.(*truncatedUptoTxAttribute).txID = txID
	return md
}

func (md *TxMetadata) Extra() []byte {
	attr, ok := md.attributes[extraAttrCode]
	if !ok {
		return nil
	}

	return attr.(*extraAttribute).extra
}

func (md *TxMetadata) WithExtra(data []byte) error {
	if len(data) == 0 {
		delete(md.attributes, extraAttrCode)
		return nil
	}

	if len(data) > maxExtraLen {
		return fmt.Errorf("%w: max extra data length exceeded", ErrIllegalArguments)
	}

	attr := &extraAttribute{extra: make([]byte, len(data))}
	copy(attr.extra, data)

	md.attributes[extraAttrCode] = attr

	return nil
}

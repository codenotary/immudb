/*
Copyright 2022 Codenotary Inc. All rights reserved.

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
	"fmt"
)

// attributeCode is used to identify the attribute.
const (
	truncatedUptoTxAttrCode attributeCode = 0
	indexingChangesAttrCode attributeCode = 1
)

// attribute size is the size of the attribute in bytes.
const (
	truncatedUptoTxAttrSize = txIDSize
	indexingChangesAttrSize = sszSize + MaxNumberOfIndexChangesPerTx*indexChangeSize
)

const maxTxMetadataLen = (attrCodeSize + truncatedUptoTxAttrSize) +
	(attrCodeSize + indexingChangesAttrSize)

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

type indexingChangesAttribute struct {
	changes map[int]IndexChange
}

type IndexChange interface {
	IsIndexDeletion() bool
	IsIndexCreation() bool
}

type IndexDeletionChange struct {
}

func (c *IndexDeletionChange) IsIndexDeletion() bool {
	return true
}

func (c *IndexDeletionChange) IsIndexCreation() bool {
	return false
}

type IndexCreationChange struct {
	InitialTxID uint64
	FinalTxID   uint64
	InitialTs   int64
	FinalTs     int64
}

func (c *IndexCreationChange) IsIndexDeletion() bool {
	return false
}

func (c *IndexCreationChange) IsIndexCreation() bool {
	return true
}

const indexChangeSize = indexIDSize + 1 /* index change type */ + indexCreationSize // size of bigger change
const indexCreationSize = 2*txIDSize + 2*tsSize

const indexDeletionChange = 0
const indexCreationChange = 1

// code returns the attribute code.
func (a *indexingChangesAttribute) code() attributeCode {
	return indexingChangesAttrCode
}

// serialize returns the serialized attribute.
func (a *indexingChangesAttribute) serialize() []byte {
	var b [indexingChangesAttrSize]byte
	i := 0

	binary.BigEndian.PutUint16(b[i:], uint16(len(a.changes)))
	i += sszSize

	for id, change := range a.changes {
		binary.BigEndian.PutUint16(b[i:], uint16(id))
		i += indexIDSize

		if change.IsIndexDeletion() {
			b[i] = indexDeletionChange
			i++
		}
		if change.IsIndexCreation() {
			b[i] = indexCreationChange
			i++

			c := change.(*IndexCreationChange)

			binary.BigEndian.PutUint64(b[i:], c.InitialTxID)
			i += txIDSize

			binary.BigEndian.PutUint64(b[i:], c.FinalTxID)
			i += txIDSize

			binary.BigEndian.PutUint64(b[i:], uint64(c.InitialTs))
			i += tsSize

			binary.BigEndian.PutUint64(b[i:], uint64(c.FinalTs))
			i += tsSize
		}
	}

	return b[:i]
}

// deserialize deserializes the attribute.
func (a *indexingChangesAttribute) deserialize(b []byte) (int, error) {
	n := 0

	if len(b) < sszSize {
		return n, ErrCorruptedData
	}

	changesCount := int(binary.BigEndian.Uint16(b[n:]))
	n += sszSize

	if changesCount > MaxNumberOfIndexChangesPerTx {
		return n, ErrCorruptedData
	}

	a.changes = make(map[int]IndexChange, changesCount)

	for i := 0; i < changesCount; i++ {
		if len(b) < indexIDSize+1 {
			return n, ErrCorruptedData
		}

		indexID := int(binary.BigEndian.Uint16(b[n:]))
		n += indexIDSize

		changeType := b[n]
		n++

		if changeType == indexDeletionChange {
			a.changes[indexID] = &IndexDeletionChange{}
			continue
		}

		if changeType == indexCreationChange {
			if len(b) < indexCreationSize {
				return n, ErrCorruptedData
			}

			change := &IndexCreationChange{}

			change.InitialTxID = binary.BigEndian.Uint64(b[n:])
			n += txIDSize

			change.FinalTxID = binary.BigEndian.Uint64(b[n:])
			n += txIDSize

			change.InitialTs = int64(binary.BigEndian.Uint64(b[n:]))
			n += tsSize

			change.FinalTs = int64(binary.BigEndian.Uint64(b[n:]))
			n += tsSize

			a.changes[indexID] = change
			continue
		}

		return n, ErrCorruptedData
	}

	return n, nil
}

func getAttributeFrom(attrCode attributeCode) (attribute, error) {
	switch attrCode {
	case truncatedUptoTxAttrCode:
		{
			return &truncatedUptoTxAttribute{}, nil
		}
	case indexingChangesAttrCode:
		{
			return &indexingChangesAttribute{}, nil
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

func (md *TxMetadata) Equal(amd *TxMetadata) bool {
	if amd == nil || md == nil {
		return false
	}
	return bytes.Equal(md.Bytes(), amd.Bytes())
}

func (md *TxMetadata) Bytes() []byte {
	var b bytes.Buffer

	for _, attrCode := range []attributeCode{truncatedUptoTxAttrCode, indexingChangesAttrCode} {
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

func (md *TxMetadata) WithIndexingChanges(indexingChanges map[int]IndexChange) *TxMetadata {
	if len(indexingChanges) == 0 {
		delete(md.attributes, indexingChangesAttrCode)
		return md
	}

	md.attributes[indexingChangesAttrCode] = &indexingChangesAttribute{
		changes: indexingChanges,
	}

	return md
}

func (md *TxMetadata) GetIndexingChanges() map[int]IndexChange {
	attr, ok := md.attributes[indexingChangesAttrCode]
	if !ok {
		return nil
	}

	return attr.(*indexingChangesAttribute).changes
}

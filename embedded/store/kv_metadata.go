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
	"errors"
	"fmt"
	"time"
)

var ErrNonExpirable = errors.New("non expirable")
var ErrReadOnly = errors.New("read-only")

const (
	deletedAttrCode   attributeCode = 0
	expiresAtAttrCode attributeCode = 1
)

const deletedAttrSize = 0
const expiresAtAttrSize = tsSize

const maxKVMetadataLen = (attrCodeSize + deletedAttrSize) + (attrCodeSize + expiresAtAttrSize)

type KVMetadata struct {
	attributes map[attributeCode]attribute
	readonly   bool
}

type deletedAttribute struct {
}

func (a *deletedAttribute) code() attributeCode {
	return deletedAttrCode
}

func (a *deletedAttribute) serialize() []byte {
	return nil
}

func (a *deletedAttribute) deserialize(b []byte) (int, error) {
	return 0, nil
}

type expiresAtAttribute struct {
	expiresAt time.Time
}

func (a *expiresAtAttribute) code() attributeCode {
	return expiresAtAttrCode
}

func (a *expiresAtAttribute) serialize() []byte {
	var b [tsSize]byte
	binary.BigEndian.PutUint64(b[:], uint64(a.expiresAt.Unix()))
	return b[:]
}

func (a *expiresAtAttribute) deserialize(b []byte) (int, error) {
	if len(b) < tsSize {
		return 0, ErrCorruptedData
	}

	a.expiresAt = time.Unix(int64(binary.BigEndian.Uint64(b)), 0)

	return tsSize, nil
}

func NewKVMetadata() *KVMetadata {
	return newKVMetadata(false)
}

func NewReadOnlyKVMetadata() *KVMetadata {
	return newKVMetadata(true)
}

func newKVMetadata(readonly bool) *KVMetadata {
	return &KVMetadata{
		attributes: make(map[attributeCode]attribute),
		readonly:   readonly,
	}
}

func (md *KVMetadata) AsDeleted(deleted bool) error {
	if md.readonly {
		return ErrReadOnly
	}

	if !deleted {
		delete(md.attributes, deletedAttrCode)
		return nil
	}

	_, ok := md.attributes[deletedAttrCode]
	if !ok {
		md.attributes[deletedAttrCode] = &deletedAttribute{}
	}

	return nil
}

func (md *KVMetadata) Deleted() bool {
	_, ok := md.attributes[deletedAttrCode]
	return ok
}

func (md *KVMetadata) ExpiresAt(expiresAt time.Time) error {
	if md.readonly {
		return ErrReadOnly
	}

	expAtAttr, ok := md.attributes[expiresAtAttrCode]
	if !ok {
		expAtAttr = &expiresAtAttribute{expiresAt: expiresAt}
		md.attributes[expiresAtAttrCode] = expAtAttr
		return nil
	}

	expAtAttr.(*expiresAtAttribute).expiresAt = expiresAt
	return nil
}

func (md *KVMetadata) NonExpirable() *KVMetadata {
	delete(md.attributes, expiresAtAttrCode)
	return md
}

func (md *KVMetadata) IsExpirable() bool {
	_, ok := md.attributes[expiresAtAttrCode]
	return ok
}

func (md *KVMetadata) ExpirationTime() (time.Time, error) {
	expAtAttr, ok := md.attributes[expiresAtAttrCode]
	if !ok {
		return time.Now(), ErrNonExpirable
	}

	return expAtAttr.(*expiresAtAttribute).expiresAt, nil
}

func (md *KVMetadata) ExpiredAt(mtime time.Time) bool {
	expAtAttr, ok := md.attributes[expiresAtAttrCode]
	if !ok {
		return false
	}

	return !expAtAttr.(*expiresAtAttribute).expiresAt.After(mtime)
}

func (md *KVMetadata) Bytes() []byte {
	var b bytes.Buffer

	for _, attrCode := range []attributeCode{deletedAttrCode, expiresAtAttrCode} {
		attr, ok := md.attributes[attrCode]
		if ok {
			b.WriteByte(byte(attr.code()))
			b.Write(attr.serialize())
		}
	}

	return b.Bytes()
}

func (md *KVMetadata) unsafeReadFrom(b []byte) error {
	if len(b) > maxKVMetadataLen {
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

		attr, err := newAttribute(attrCode)
		if err != nil {
			return err
		}

		n, err := attr.deserialize(b[i:])
		if err != nil {
			return fmt.Errorf("error reading metadata attributes: %w", err)
		}

		i += n

		md.attributes[attr.code()] = attr
	}

	return nil
}

func newAttribute(attrCode attributeCode) (attribute, error) {
	switch attrCode {
	case deletedAttrCode:
		{
			return &deletedAttribute{}, nil
		}
	case expiresAtAttrCode:
		{
			return &expiresAtAttribute{}, nil
		}
	default:
		{
			return nil, fmt.Errorf("error reading metadata attributes: %w", ErrCorruptedData)
		}
	}
}

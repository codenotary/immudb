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
	"encoding/binary"
	"time"
)

const maxKVMetadataLen = 1 + tsSize

type KVMetadata struct {
	deleted   bool
	expiresAt *time.Time
}

func NewKVMetadata() *KVMetadata {
	return &KVMetadata{}
}

func (md *KVMetadata) AsDeleted(deleted bool) *KVMetadata {
	md.deleted = deleted
	return md
}

func (md *KVMetadata) Deleted() bool {
	return md.deleted
}

func (md *KVMetadata) ExpiresAt(expiresAt *time.Time) *KVMetadata {
	md.expiresAt = expiresAt
	return md
}

func (md *KVMetadata) Expirable() bool {
	return md.expiresAt != nil
}

func (md *KVMetadata) ExpirationTime() *time.Time {
	return md.expiresAt
}

func (md *KVMetadata) ExpiredAt(mtime time.Time) bool {
	if !md.Expirable() {
		return false
	}

	return !md.expiresAt.After(mtime)
}

func (md *KVMetadata) Bytes() []byte {
	var b [1 + tsSize]byte

	if md.deleted {
		b[0] = 1
	}

	if md.expiresAt == nil {
		return b[:1]
	}

	binary.BigEndian.PutUint64(b[1:], uint64(md.expiresAt.Unix()))

	return b[:]
}

func (md *KVMetadata) ReadFrom(b []byte) error {
	if len(b) == 0 {
		return nil
	}

	md.deleted = b[0] != 0

	if len(b) == 1 {
		return nil
	}

	if len(b) != 1+tsSize {
		return ErrCorruptedData
	}

	ts := time.Unix(int64(binary.BigEndian.Uint64(b[1:])), 0)
	md.expiresAt = &ts

	return nil
}

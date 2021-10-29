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

const maxKVMetadataLen = 1

type KVMetadata struct {
	deleted bool
}

const deletedFlag = 1 << 7

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

func (md *KVMetadata) Bytes() []byte {
	var b byte

	if md.deleted {
		b = b | deletedFlag
	}

	return []byte{b}
}

func (md *KVMetadata) ReadFrom(b []byte) error {
	if len(b) == 0 {
		return nil
	}

	md.deleted = b[0]&deletedFlag != 0

	return nil
}

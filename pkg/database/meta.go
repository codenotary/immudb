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

package database

import (
	"encoding/binary"
	"math"

	"github.com/codenotary/immudb/embedded/store"
)

const (
	SetKeyPrefix byte = iota
	SortedSetKeyPrefix
	SQLPrefix
	DocumentPrefix
)

const (
	PlainValuePrefix = iota
	ReferenceValuePrefix
)

// WrapWithPrefix ...
func WrapWithPrefix(b []byte, prefix byte) []byte {
	wb := make([]byte, 1+len(b))
	wb[0] = prefix
	copy(wb[1:], b)
	return wb
}

func TrimPrefix(prefixed []byte) []byte {
	return prefixed[1:]
}

func EncodeKey(key []byte) []byte {
	return WrapWithPrefix(key, SetKeyPrefix)
}

func EncodeEntrySpec(
	key []byte,
	md *store.KVMetadata,
	value []byte,
) *store.EntrySpec {
	return &store.EntrySpec{
		Key:      WrapWithPrefix(key, SetKeyPrefix),
		Metadata: md,
		Value:    WrapWithPrefix(value, PlainValuePrefix),
	}
}

func EncodeReference(
	key []byte,
	md *store.KVMetadata,
	referencedKey []byte,
	atTx uint64,
) *store.EntrySpec {
	// Note: metadata record may be used as reference holder, reference resolution would be faster
	// It may be introduced in a backward-compatible way i.e. if not present in metadata then resolve by reading value
	return &store.EntrySpec{
		Key:      WrapWithPrefix(key, SetKeyPrefix),
		Metadata: md,
		Value:    WrapReferenceValueAt(WrapWithPrefix(referencedKey, SetKeyPrefix), atTx),
	}
}

func WrapReferenceValueAt(key []byte, atTx uint64) []byte {
	refVal := make([]byte, 1+8+len(key))

	refVal[0] = ReferenceValuePrefix
	binary.BigEndian.PutUint64(refVal[1:], atTx)
	copy(refVal[1+8:], key)

	return refVal
}

func EncodeZAdd(set []byte, score float64, key []byte, atTx uint64) *store.EntrySpec {
	return &store.EntrySpec{
		Key:   WrapZAddReferenceAt(set, score, key, atTx),
		Value: nil,
	}
}

func WrapZAddReferenceAt(set []byte, score float64, key []byte, atTx uint64) []byte {
	zKey := make([]byte, 1+setLenLen+len(set)+scoreLen+keyLenLen+len(key)+txIDLen)
	zi := 0

	zKey[0] = SortedSetKeyPrefix
	zi++
	binary.BigEndian.PutUint64(zKey[zi:], uint64(len(set)))
	zi += setLenLen
	copy(zKey[zi:], set)
	zi += len(set)
	binary.BigEndian.PutUint64(zKey[zi:], math.Float64bits(score))
	zi += scoreLen
	binary.BigEndian.PutUint64(zKey[zi:], uint64(len(key)))
	zi += keyLenLen
	copy(zKey[zi:], key)
	zi += len(key)
	binary.BigEndian.PutUint64(zKey[zi:], atTx)

	return zKey
}

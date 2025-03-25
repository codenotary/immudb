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

package stream

import (
	"bytes"
	"encoding/binary"
	"io"

	"github.com/codenotary/immudb/pkg/api/schema"
)

var ProveSinceTxFakeKey = []byte("ProveSinceTx")

type KeyValue struct {
	Key   *ValueSize
	Value *ValueSize
}

type ValueSize struct {
	Content io.Reader
	Size    int
}

type VerifiableEntry struct {
	EntryWithoutValueProto *ValueSize
	VerifiableTxProto      *ValueSize
	InclusionProofProto    *ValueSize
	Value                  *ValueSize
}

type ZEntry struct {
	Set   *ValueSize
	Key   *ValueSize
	Score *ValueSize
	AtTx  *ValueSize
	Value *ValueSize
}

const (
	TOp_Kv byte = 1 << iota
	TOp_ZAdd
	TOp_Ref
)

type IsOp_Operation interface {
	isOp_Operation()
}

type Op struct {
	Operation IsOp_Operation
}

type Op_ZAdd struct {
	ZAdd *schema.ZAddRequest
}
type Op_KeyValue struct {
	KeyValue *KeyValue
}
type Op_Ref struct{}

func (*Op_ZAdd) isOp_Operation()     {}
func (*Op_KeyValue) isOp_Operation() {}
func (*Op_Ref) isOp_Operation()      {}

type ExecAllRequest struct {
	Operations []*Op
}

// NumberToBytes ...
func NumberToBytes(n interface{}) ([]byte, error) {
	var buf bytes.Buffer
	err := binary.Write(&buf, binary.BigEndian, n)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), err
}

// NumberFromBytes ...
func NumberFromBytes(bs []byte, n interface{}) error {
	buf := bytes.NewReader(bs)
	return binary.Read(buf, binary.BigEndian, n)
}

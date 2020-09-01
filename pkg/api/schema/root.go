/*
Copyright 2019-2020 vChain, Inc.

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

package schema

import (
	"errors"
	"github.com/codenotary/immudb/pkg/signer"
	"github.com/golang/protobuf/proto"
)

func NewRoot() *Root {
	return &Root{Payload: &RootIndex{}, Signature: &Signature{}}
}

//CheckSignature
func (r *Root) CheckSignature() (ok bool, err error) {
	if r.Signature.PublicKey == nil {
		return false, errors.New("no public key found")
	}
	if r.Signature.Signature == nil {
		return false, errors.New("no signature found")
	}
	var m []byte
	if m, err = proto.Marshal(r.Payload); err != nil {
		return ok, err
	}
	return signer.Verify(m, r.Signature.Signature, r.Signature.PublicKey)
}

func (r *Root) GetIndex() uint64 {
	return r.GetPayload().GetIndex()
}

func (r *Root) GetRoot() []byte {
	return r.GetPayload().GetRoot()
}

func (r *Root) SetIndex(index uint64) {
	if r.Payload == nil {
		r.Payload = &RootIndex{}
	}
	r.Payload.Index = index
}

func (r *Root) SetRoot(root []byte) {
	if r.Payload == nil {
		r.Payload = &RootIndex{}
	}
	r.Payload.Root = root
}

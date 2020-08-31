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

package server

import (
	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/codenotary/immudb/pkg/signer"
	"github.com/golang/protobuf/proto"
)

type RootSigner interface {
	Sign(root *schema.Root) (*schema.Root, error)
}

type rootSigner struct {
	Signer signer.Signer
}

func NewRootSigner(signer signer.Signer) *rootSigner {
	return &rootSigner{
		Signer: signer,
	}
}

func (rs *rootSigner) Sign(root *schema.Root) (*schema.Root, error) {
	if m, err := proto.Marshal(root.Payload); err != nil {
		return nil, err
	} else {
		root.Signature.Signature, root.Signature.PublicKey, err = rs.Signer.Sign(m)
	}
	return root, nil
}

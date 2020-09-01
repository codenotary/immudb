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
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestNewRootSigner(t *testing.T) {
	s, _ := signer.NewSigner("./../../test/signer/ec3.key")
	rs := NewRootSigner(s)
	assert.IsType(t, &rootSigner{}, rs)
}

func TestRootSigner_Sign(t *testing.T) {
	s, _ := signer.NewSigner("./../../test/signer/ec3.key")
	rs := NewRootSigner(s)
	root := schema.NewRoot()
	root, err := rs.Sign(root)
	assert.NoError(t, err)
	assert.IsType(t, &schema.Root{}, root)
}

func TestRootSigner_Err(t *testing.T) {
	s, _ := signer.NewSigner("./../../test/signer/ec3.key")
	rs := NewRootSigner(s)
	root := schema.NewRoot()
	root.Payload = nil
	root, err := rs.Sign(root)
	assert.Error(t, err)
}

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
	"context"
	"log"
	"testing"

	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/codenotary/immudb/pkg/signer"
	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/types/known/emptypb"
)

func TestServerCurrentRootSigned(t *testing.T) {
	dbRootpath := DefaultOption().GetDbRootPath()
	s := DefaultServer()

	sig, err := signer.NewSigner("./../../test/signer/ec3.key")
	assert.NoError(t, err)

	rsig := NewRootSigner(sig)
	s = s.WithOptions(s.Options.WithAuth(false).WithInMemoryStore(true).WithSigningKey("foo")).WithRootSigner(rsig).(*ImmuServer)
	err = s.loadDefaultDatabase(dbRootpath)
	if err != nil {
		log.Fatal(err)
	}
	err = s.loadSystemDatabase(dbRootpath, s.Options.AdminPassword)
	if err != nil {
		log.Fatal(err)
	}

	ctx := context.Background()

	_, _ = s.SafeSet(ctx, &schema.SafeSetOptions{
		Kv: &schema.KeyValue{
			Key:   []byte("Alberto"),
			Value: []byte("Tomba"),
		},
	})
	root, err := s.CurrentRoot(ctx, &emptypb.Empty{})

	assert.NoError(t, err)
	assert.IsType(t, &schema.Root{}, root)
	assert.IsType(t, &schema.Signature{}, root.Signature)
	assert.NotNil(t, root.Signature.Signature)
	assert.NotNil(t, root.Signature.PublicKey)

	p2verify, err := proto.Marshal(root.Payload)
	assert.NoError(t, err)

	ok, err := signer.Verify(p2verify, root.Signature.Signature, root.Signature.PublicKey)

	assert.NoError(t, err)
	assert.True(t, ok)
}

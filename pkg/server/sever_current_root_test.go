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
	"github.com/codenotary/immudb/pkg/database"
	"github.com/codenotary/immudb/pkg/signer"
	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/types/known/emptypb"
)

func TestServerCurrentRootSigned(t *testing.T) {
	dbRootpath := database.DefaultOption().GetDbRootPath()
	s := DefaultServer()

	sig, err := signer.NewSigner("./../../test/signer/ec3.key")
	assert.NoError(t, err)

	stSig := NewStateSigner(sig)
	s = s.WithOptions(s.Options.WithAuth(false).WithSigningKey("foo")).WithStateSigner(stSig).(*ImmuServer)
	err = s.loadDefaultDatabase(dbRootpath)
	if err != nil {
		log.Fatal(err)
	}
	err = s.loadSystemDatabase(dbRootpath, s.Options.AdminPassword)
	if err != nil {
		log.Fatal(err)
	}

	ctx := context.Background()

	_, _ = s.Set(ctx, &schema.SetRequest{
		KVs: []*schema.KeyValue{
			{
				Key:   []byte("Alberto"),
				Value: []byte("Tomba"),
			},
		},
	},
	)

	state, err := s.CurrentImmutableState(ctx, &emptypb.Empty{})

	assert.NoError(t, err)
	assert.IsType(t, &schema.ImmutableState{}, state)
	assert.IsType(t, &schema.Signature{}, state.Signature)
	assert.NotNil(t, state.Signature.Signature)
	assert.NotNil(t, state.Signature.PublicKey)

	ok, err := signer.Verify(state.ToBytes(), state.Signature.Signature, state.Signature.PublicKey)

	assert.NoError(t, err)
	assert.True(t, ok)
}

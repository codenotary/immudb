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

package signer

import (
	"crypto/sha256"
	"crypto/x509"
	"encoding/asn1"
	"encoding/pem"
	"github.com/stretchr/testify/assert"
	"io/ioutil"
	"math/big"
	"strings"
	"testing"
)

func TestNewSigner(t *testing.T) {
	s, err := NewSigner("./../../test/signer/ec3.key")
	assert.NoError(t, err)
	var i interface{} = s
	_, ok := i.(Signer)
	assert.True(t, ok)
}

func TestNewSignerFromPKey(t *testing.T) {
	privateKeyBytes, _ := ioutil.ReadFile("./../../test/signer/ec3.key")
	privateKeyBlock, _ := pem.Decode(privateKeyBytes)
	pk, _ := x509.ParseECPrivateKey(privateKeyBlock.Bytes)
	r := strings.NewReader("")
	s := NewSignerFromPKey(r, pk)
	var i interface{} = s
	_, ok := i.(Signer)
	assert.True(t, ok)
}

func TestNewSignerKeyNotExistent(t *testing.T) {
	s, err := NewSigner("./not_exists")
	assert.Error(t, err)
	assert.Nil(t, s)
}

func TestNewSignerNoKeyFound(t *testing.T) {
	s, err := NewSigner("./../../test/signer/unparsable.key")
	assert.Error(t, err)
	assert.Nil(t, s)
}

func TestNewSignerKeyUnparsable(t *testing.T) {
	s, err := NewSigner("./../../test/signer/ec3.pub")
	assert.Error(t, err)
	assert.Nil(t, s)
}

func TestSignature_Sign(t *testing.T) {
	s, err := NewSigner("./../../test/signer/ec3.key")
	rawMessage := sha256.Sum256([]byte(`myhash`))
	_, _, err = s.Sign(rawMessage[:])
	assert.NoError(t, err)
}

func TestSignature_SignError(t *testing.T) {
	privateKeyBytes, _ := ioutil.ReadFile("./../../test/signer/ec3.key")
	privateKeyBlock, _ := pem.Decode(privateKeyBytes)
	pk, _ := x509.ParseECPrivateKey(privateKeyBlock.Bytes)

	r := strings.NewReader("")
	s := NewSignerFromPKey(r, pk)
	_, _, err := s.Sign([]byte(``))
	assert.Error(t, err)
}

func TestSignature_Verify(t *testing.T) {
	s, err := NewSigner("./../../test/signer/ec3.key")

	rawMessage := sha256.Sum256([]byte(`myhash`))
	signature, publicKey, _ := s.Sign(rawMessage[:])
	ok, err := Verify(rawMessage[:], signature, publicKey)
	assert.True(t, ok)
	assert.NoError(t, err)
}

func TestSignature_VerifyError(t *testing.T) {
	s, err := NewSigner("./../../test/signer/ec3.key")

	rawMessage := sha256.Sum256([]byte(`myhash`))
	_, publicKey, _ := s.Sign(rawMessage[:])
	ok, err := Verify(rawMessage[:], []byte(`wrongsignature`), publicKey)
	assert.False(t, ok)
	assert.Error(t, err)
}

func TestSignature_VerifyFalse(t *testing.T) {
	s, err := NewSigner("./../../test/signer/ec3.key")
	rawMessage := sha256.Sum256([]byte(`myhash`))
	_, publicKey, _ := s.Sign(rawMessage[:])
	sigToMarshal := ecdsaSignature{R: &big.Int{}, S: &big.Int{}}
	m, _ := asn1.Marshal(sigToMarshal)
	ok, err := Verify(rawMessage[:], m, publicKey)
	assert.False(t, ok)
	assert.NoError(t, err)
}

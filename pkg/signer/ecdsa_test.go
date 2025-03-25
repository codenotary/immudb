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

package signer

import (
	"crypto/sha256"
	"crypto/x509"
	"encoding/asn1"
	"encoding/pem"
	"io"
	"io/ioutil"
	"math/big"
	"strings"
	"syscall"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNewSigner(t *testing.T) {
	s, err := NewSigner("./../../test/signer/ec3.key")
	require.NoError(t, err)
	var i interface{} = s
	_, ok := i.(Signer)
	require.True(t, ok)
}

func TestNewSignerFromPKey(t *testing.T) {
	privateKeyBytes, _ := ioutil.ReadFile("./../../test/signer/ec3.key")
	privateKeyBlock, _ := pem.Decode(privateKeyBytes)
	pk, _ := x509.ParseECPrivateKey(privateKeyBlock.Bytes)
	r := strings.NewReader("")
	s := NewSignerFromPKey(r, pk)
	var i interface{} = s
	_, ok := i.(Signer)
	require.True(t, ok)
}

func TestNewSignerKeyNotExistent(t *testing.T) {
	s, err := NewSigner("./not_exists")
	require.ErrorIs(t, err, syscall.ENOENT)
	require.Nil(t, s)
}

func TestNewSignerNoKeyFound(t *testing.T) {
	s, err := NewSigner("./../../test/signer/unparsable.key")
	require.ErrorContains(t, err, "no ecdsa key found in provided signing key file")
	require.Nil(t, s)
}

func TestNewSignerKeyUnparsable(t *testing.T) {
	s, err := NewSigner("./../../test/signer/ec3.pub")
	require.ErrorContains(t, err, "x509: failed to parse EC private key")
	require.Nil(t, s)
}

func TestSignature_Sign(t *testing.T) {
	s, err := NewSigner("./../../test/signer/ec3.key")
	require.NoError(t, err)
	rawMessage := sha256.Sum256([]byte(`myhash`))
	_, _, err = s.Sign(rawMessage[:])
	require.NoError(t, err)
}

func TestSignature_SignError(t *testing.T) {
	privateKeyBytes, _ := ioutil.ReadFile("./../../test/signer/ec3.key")
	privateKeyBlock, _ := pem.Decode(privateKeyBytes)
	pk, _ := x509.ParseECPrivateKey(privateKeyBlock.Bytes)

	r := strings.NewReader("")
	s := NewSignerFromPKey(r, pk)
	_, _, err := s.Sign([]byte(``))
	require.ErrorIs(t, err, io.EOF)
}

func TestSignature_Verify(t *testing.T) {
	s, err := NewSigner("./../../test/signer/ec3.key")
	require.NoError(t, err)

	rawMessage := sha256.Sum256([]byte(`myhash`))
	signature, publicKey, _ := s.Sign(rawMessage[:])
	ecdsaPK, err := UnmarshalKey(publicKey)
	require.NoError(t, err)
	err = Verify(rawMessage[:], signature, ecdsaPK)
	require.NoError(t, err)
}

func TestSignature_VerifyError(t *testing.T) {
	s, err := NewSigner("./../../test/signer/ec3.key")
	require.NoError(t, err)

	rawMessage := sha256.Sum256([]byte(`myhash`))
	_, publicKey, _ := s.Sign(rawMessage[:])
	ecdsaPK, err := UnmarshalKey(publicKey)
	require.NoError(t, err)
	err = Verify(rawMessage[:], []byte(`wrongsignature`), ecdsaPK)
	require.ErrorIs(t, err, ErrKeyCannotBeVerified)
}

func TestSignature_VerifyFalse(t *testing.T) {
	s, err := NewSigner("./../../test/signer/ec3.key")
	require.NoError(t, err)
	rawMessage := sha256.Sum256([]byte(`myhash`))
	_, publicKey, _ := s.Sign(rawMessage[:])
	sigToMarshal := ecdsaSignature{R: &big.Int{}, S: &big.Int{}}
	m, _ := asn1.Marshal(sigToMarshal)
	ecdsaPK, err := UnmarshalKey(publicKey)
	require.NoError(t, err)
	err = Verify(rawMessage[:], m, ecdsaPK)
	require.ErrorIs(t, err, ErrKeyCannotBeVerified)
}

func TestUnmarshalKey_Error(t *testing.T) {
	ecdsaPK, err := UnmarshalKey([]byte(`wrongkey`))
	require.Nil(t, ecdsaPK)
	require.ErrorIs(t, err, ErrInvalidPublicKey)
}

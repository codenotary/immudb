/*
Copyright 2024 Codenotary Inc. All rights reserved.

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
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/x509"
	"encoding/asn1"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"math/big"
)

var ErrInvalidPublicKey = errors.New("invalid public key")
var ErrKeyCannotBeVerified = errors.New("key cannot be verified")

type signer struct {
	rand       io.Reader
	privateKey *ecdsa.PrivateKey
}

type ecdsaSignature struct {
	R *big.Int
	S *big.Int
}

// NewSigner returns a signer object. It requires a private key file path. To generate a valid key use openssl tool. Ex: openssl ecparam -name prime256v1 -genkey -noout -out ec.key
func NewSigner(privateKeyPath string) (Signer, error) {
	privateKeyBytes, err := ioutil.ReadFile(privateKeyPath)
	if err != nil {
		return nil, err
	}
	privateKeyBlock, _ := pem.Decode(privateKeyBytes)
	if privateKeyBlock == nil {
		return nil, errors.New("no ecdsa key found in provided signing key file")
	}
	privateKey, err := x509.ParseECPrivateKey(privateKeyBlock.Bytes)
	if err != nil {
		return nil, err
	}
	return signer{rand: rand.Reader, privateKey: privateKey}, nil
}

// NewSignerFromPKey returns a signer from a io.Reader and a *ecdsa.PrivateKey.
func NewSignerFromPKey(r io.Reader, pk *ecdsa.PrivateKey) Signer {
	return signer{rand: r, privateKey: pk}
}

// sign sign a payload and returns asn1 marshal byte sequence
func (sig signer) Sign(payload []byte) ([]byte, []byte, error) {
	hash := sha256.Sum256(payload)
	r, s, err := ecdsa.Sign(sig.rand, sig.privateKey, hash[:])
	if err != nil {
		return nil, nil, err
	}
	sigToMarshal := ecdsaSignature{R: r, S: s}
	m, _ := asn1.Marshal(sigToMarshal)
	pk := sig.privateKey.PublicKey
	p := elliptic.Marshal(pk.Curve, pk.X, pk.Y)

	return m, p, nil
}

func UnmarshalKey(publicKey []byte) (*ecdsa.PublicKey, error) {
	x, y := elliptic.Unmarshal(elliptic.P256(), publicKey)
	if x == nil {
		return nil, ErrInvalidPublicKey
	}
	return &ecdsa.PublicKey{Curve: elliptic.P256(), X: x, Y: y}, nil
}

// verify verifies a signed payload
func Verify(payload []byte, signature []byte, publicKey *ecdsa.PublicKey) error {
	hash := sha256.Sum256(payload)
	es := ecdsaSignature{}
	if _, err := asn1.Unmarshal(signature, &es); err != nil {
		return fmt.Errorf("%w: %v", ErrKeyCannotBeVerified, err)
	}
	if !ecdsa.Verify(publicKey, hash[:], es.R, es.S) {
		return ErrKeyCannotBeVerified
	}
	return nil
}

func ParsePublicKeyFile(filePath string) (*ecdsa.PublicKey, error) {
	publicKeyBytes, err := ioutil.ReadFile(filePath)
	if err != nil {
		return nil, err
	}
	publicKeyBlock, _ := pem.Decode(publicKeyBytes)
	if publicKeyBlock == nil {
		return nil, errors.New("no ecdsa key found in provided public key file")
	}
	cert, err := x509.ParsePKIXPublicKey(publicKeyBlock.Bytes)
	if err != nil {
		return nil, err
	}
	return cert.(*ecdsa.PublicKey), nil
}

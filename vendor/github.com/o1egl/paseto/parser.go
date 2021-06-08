// Package paseto provides a Go implementation of PASETO, a secure alternative
// to the JOSE standards (JWT, JWE, JWS). See https://paseto.io/
package paseto

import (
	"crypto"
	"strings"

	"github.com/pkg/errors"
)

// Version defines the token version.
type Version string

// Purpose defines the token type by its intended purpose.
type Purpose int

const (
	// Version1 defines protocol version 1
	Version1 = Version("v1")
	// Version2 defines protocol version 2
	Version2 = Version("v2")
)

const (
	// LOCAL defines symmetric encrypted token type
	LOCAL Purpose = iota
	// PUBLIC defines asymmetric signed token type
	PUBLIC
)

var availableVersions = map[Version]Protocol{
	Version1: NewV1(),
	Version2: NewV2(),
}

// Parse extracts the payload and footer from the token by calling either
// Decrypt() or Verify(), depending on whether the token is public or private.
// To parse public tokens you need to provide a map containing v1 and/or v2
// public keys, depending on the version of the token. To parse private tokens
// you need to provide the symmetric key.
func Parse(token string, payload interface{}, footer interface{},
	symmetricKey []byte, publicKeys map[Version]crypto.PublicKey) (Version, error) {
	parts := strings.Split(token, ".")
	version := Version(parts[0])
	if len(parts) < 3 {
		return version, ErrIncorrectTokenFormat
	}

	protocol, found := availableVersions[version]
	if !found {
		return version, ErrUnsupportedTokenVersion
	}

	switch parts[1] {
	case "local":
		return version, protocol.Decrypt(token, symmetricKey, payload, footer)
	case "public":
		pubKey, found := publicKeys[version]
		if !found {
			return version, ErrPublicKeyNotFound
		}
		return version, protocol.Verify(token, pubKey, payload, footer)
	default:
		return version, ErrUnsupportedTokenType

	}
}

// ParseFooter parses the footer from the token and returns it.
func ParseFooter(token string, footer interface{}) error {
	parts := strings.Split(token, ".")
	if len(parts) == 4 {
		b, err := tokenEncoder.DecodeString(parts[3])
		if err != nil {
			return errors.Wrap(err, "failed to decode token")
		}
		return errors.Wrap(fillValue(b, footer), "failed to decode footer")
	}
	if len(parts) < 3 {
		return ErrIncorrectTokenFormat
	}
	return nil
}

// GetTokenInfo returns the token version (paseto.Version1 or paseto.Version2) and purpose
// (paseto.LOCAL or paseto.PUBLIC).
func GetTokenInfo(token string) (Version, Purpose, error) {
	parts := strings.Split(token, ".")
	if len(parts) < 3 {
		return "", 0, ErrIncorrectTokenFormat
	}

	var version Version
	var purpose Purpose

	switch parts[0] {
	case string(Version1):
		version = Version1
	case string(Version2):
		version = Version2
	default:
		return "", 0, ErrUnsupportedTokenVersion
	}

	switch parts[1] {
	case "local":
		purpose = LOCAL
	case "public":
		purpose = PUBLIC
	default:
		return "", 0, ErrUnsupportedTokenType
	}

	return version, purpose, nil
}

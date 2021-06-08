package paseto

import (
	"crypto"
	"crypto/rand"
	"io"

	"github.com/aead/chacha20/chacha"
	"github.com/aead/chacha20poly1305"
	"github.com/pkg/errors"
	"golang.org/x/crypto/blake2b"
	"golang.org/x/crypto/ed25519"
)

const (
	v2SignSize = ed25519.SignatureSize
)

var headerV2 = []byte("v2.local.")
var headerV2Public = []byte("v2.public.")

// NewV2 returns a v2 implementation of PASETO tokens.
func NewV2() *V2 {
	return &V2{}
}

// V2 is a v2 implementation of PASETO tokens
type V2 struct {
	// this property is used for testing purposes only
	nonce []byte
}

// Encrypt implements Protocol.Encrypt
func (p *V2) Encrypt(key []byte, payload interface{}, footer interface{}) (string, error) {
	payloadBytes, err := infToByteArr(payload)
	if err != nil {
		return "", errors.Wrap(err, "failed to encode payload to []byte")
	}

	footerBytes, err := infToByteArr(footer)
	if err != nil {
		return "", errors.Wrap(err, "failed to encode footer to []byte")
	}

	var rndBytes []byte

	if p.nonce != nil {
		rndBytes = p.nonce
	} else {
		rndBytes = make([]byte, chacha.XNonceSize)
		if _, err := io.ReadFull(rand.Reader, rndBytes); err != nil {
			return "", errors.Wrap(err, "failed to read from rand.Reader")
		}
	}

	hash, err := blake2b.New(chacha.XNonceSize, rndBytes)
	if err != nil {
		return "", errors.Wrap(err, "failed to create blake2b hash")
	}
	if _, err := hash.Write(payloadBytes); err != nil {
		return "", errors.Wrap(err, "failed to hash payload")
	}

	nonce := hash.Sum(nil)

	aead, err := chacha20poly1305.NewXCipher(key)
	if err != nil {
		return "", errors.Wrap(err, "failed to create chacha20poly1305 cipher")
	}

	encryptedPayload := aead.Seal(payloadBytes[:0], nonce, payloadBytes, preAuthEncode(headerV2, nonce, footerBytes))

	return createToken(headerV2, append(nonce, encryptedPayload...), footerBytes), nil
}

// Decrypt implements Protocol.Decrypt
func (*V2) Decrypt(token string, key []byte, payload interface{}, footer interface{}) error {
	body, footerBytes, err := splitToken([]byte(token), headerV2)
	if err != nil {
		return errors.Wrap(err, "failed to decode token")
	}

	if len(body) < chacha.XNonceSize {
		return errors.Wrap(ErrIncorrectTokenFormat, "incorrect token size")
	}

	nonce := body[:chacha.XNonceSize]
	encryptedPayload := body[chacha.XNonceSize:]

	aead, err := chacha20poly1305.NewXCipher(key)
	if err != nil {
		return errors.Wrap(err, "failed to create chacha20poly1305 cipher")
	}

	decryptedPayload, err := aead.Open(encryptedPayload[:0], nonce, encryptedPayload, preAuthEncode(headerV2, nonce, footerBytes))
	if err != nil {
		return ErrInvalidTokenAuth
	}

	if payload != nil {
		if err := fillValue(decryptedPayload, payload); err != nil {
			return errors.Wrap(err, "failed to decode payload")
		}
	}

	if footer != nil {
		if err := fillValue(footerBytes, footer); err != nil {
			return errors.Wrap(err, "failed to decode footer")
		}
	}
	return nil
}

// Sign implements Protocol.Sign
func (*V2) Sign(privateKey crypto.PrivateKey, payload interface{}, footer interface{}) (string, error) {
	key, ok := privateKey.(ed25519.PrivateKey)
	if !ok {
		return "", ErrIncorrectPrivateKeyType
	}

	payloadBytes, err := infToByteArr(payload)
	if err != nil {
		return "", errors.Wrap(err, "failed to encode payload to []byte")
	}

	footerBytes, err := infToByteArr(footer)
	if err != nil {
		return "", errors.Wrap(err, "failed to encode footer to []byte")
	}

	sig := ed25519.Sign(key, preAuthEncode(headerV2Public, payloadBytes, footerBytes))

	return createToken(headerV2Public, append(payloadBytes, sig...), footerBytes), nil
}

// Verify implements Protocol.Verify
func (*V2) Verify(token string, publicKey crypto.PublicKey, payload interface{}, footer interface{}) error {
	pub, ok := publicKey.(ed25519.PublicKey)
	if !ok {
		return ErrIncorrectPublicKeyType
	}

	data, footerBytes, err := splitToken([]byte(token), headerV2Public)
	if err != nil {
		return errors.Wrap(err, "failed to decode token")
	}

	if len(data) < v2SignSize {
		return errors.Wrap(ErrIncorrectTokenFormat, "incorrect token size")
	}

	payloadBytes := data[:len(data)-v2SignSize]
	signature := data[len(data)-v2SignSize:]

	if !ed25519.Verify(pub, preAuthEncode(headerV2Public, payloadBytes, footerBytes), signature) {
		return ErrInvalidSignature
	}

	if payload != nil {
		if err := fillValue(payloadBytes, payload); err != nil {
			return errors.Wrap(err, "failed to decode payload")
		}
	}

	if footer != nil {
		if err := fillValue(footerBytes, footer); err != nil {
			return errors.Wrap(err, "failed to decode footer")
		}
	}

	return nil
}

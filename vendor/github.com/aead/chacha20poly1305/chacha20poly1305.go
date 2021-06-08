// Copyright (c) 2017 Andreas Auernhammer. All rights reserved.
// Use of this source code is governed by a license that can be
// found in the LICENSE file.

package chacha20poly1305

import (
	"crypto/cipher"
	"errors"

	"crypto/subtle"
	"encoding/binary"

	"github.com/aead/chacha20/chacha"
	"github.com/aead/poly1305"
)

// KeySize is the size of the key used by this AEAD, in bytes.
const KeySize = 32

var (
	errBadKeySize = errors.New("chacha20poly1305: bad key length")
	errAuthFailed = errors.New("chacha20poly1305: message authentication failed")
)

type c20p1305 struct {
	key       [32]byte
	noncesize int
}

func newCipher(key []byte, noncesize int) (cipher.AEAD, error) {
	if len(key) != KeySize {
		return nil, errBadKeySize
	}
	c := &c20p1305{
		noncesize: noncesize,
	}
	copy(c.key[:], key)
	return c, nil
}

// NewCipher returns a cipher.AEAD implementing the
// ChaCha20Poly1305 construction specified in RFC 7539 with a
// 128 bit auth. tag.
func NewCipher(key []byte) (cipher.AEAD, error) {
	return newCipher(key, chacha.NonceSize)
}

// NewIETFCipher returns a cipher.AEAD implementing the
// ChaCha20Poly1305 construction specified in RFC 7539 with a
// 128 bit auth. tag.
func NewIETFCipher(key []byte) (cipher.AEAD, error) {
	return newCipher(key, chacha.INonceSize)
}

// NewXCipher returns a cipher.AEAD implementing the
// XChaCha20Poly1305 construction specified in RFC 7539 with a
// 128 bit auth. tag.
func NewXCipher(key []byte) (cipher.AEAD, error) {
	return newCipher(key, chacha.XNonceSize)
}

func (c *c20p1305) Overhead() int { return poly1305.TagSize }

func (c *c20p1305) NonceSize() int { return c.noncesize }

func (c *c20p1305) Seal(dst, nonce, plaintext, additionalData []byte) []byte {
	if len(nonce) != c.NonceSize() {
		panic("chacha20poly1305: bad nonce length passed to Seal")
	}
	if c.NonceSize() == chacha.INonceSize && uint64(len(plaintext)) > (1<<38)-64 {
		panic("chacha20poly1305: plaintext too large")
	}

	var polyKey [32]byte
	cipher, _ := chacha.NewCipher(nonce, c.key[:], 20)
	cipher.XORKeyStream(polyKey[:], polyKey[:])
	cipher.SetCounter(1)

	n := len(plaintext)
	ret, out := sliceForAppend(dst, n+c.Overhead())
	cipher.XORKeyStream(out, plaintext)

	var tag, pad [16]byte
	hash := poly1305.New(polyKey)

	hash.Write(additionalData)
	if padAdd := len(additionalData) % 16; padAdd > 0 {
		hash.Write(pad[:16-padAdd])
	}

	hash.Write(out[:n])
	if padCt := n % 16; padCt > 0 {
		hash.Write(pad[:16-padCt])
	}

	binary.LittleEndian.PutUint64(pad[:], uint64(len(additionalData)))
	binary.LittleEndian.PutUint64(pad[8:], uint64(n))
	hash.Write(pad[:])

	hash.Sum(tag[:0])
	copy(out[n:], tag[:])
	return ret
}

func (c *c20p1305) Open(dst, nonce, ciphertext, additionalData []byte) ([]byte, error) {
	if len(nonce) != c.NonceSize() {
		panic("chacha20poly1305: bad nonce length passed to Open")
	}
	if len(ciphertext) < c.Overhead() {
		return nil, errAuthFailed
	}
	if c.NonceSize() == chacha.INonceSize && uint64(len(ciphertext)) > (1<<38)-48 {
		panic("chacha20poly1305: ciphertext too large")
	}

	var polyKey [32]byte
	cipher, _ := chacha.NewCipher(nonce, c.key[:], 20)
	cipher.XORKeyStream(polyKey[:], polyKey[:])
	cipher.SetCounter(1)

	n := len(ciphertext) - c.Overhead()

	var tag, pad [16]byte
	hash := poly1305.New(polyKey)

	hash.Write(additionalData)
	if padAdd := len(additionalData) % 16; padAdd > 0 {
		hash.Write(pad[:16-padAdd])
	}

	hash.Write(ciphertext[:n])
	if padCt := n % 16; padCt > 0 {
		hash.Write(pad[:16-padCt])
	}

	binary.LittleEndian.PutUint64(pad[:], uint64(len(additionalData)))
	binary.LittleEndian.PutUint64(pad[8:], uint64(n))
	hash.Write(pad[:])
	hash.Sum(tag[:0])

	if subtle.ConstantTimeCompare(tag[:], ciphertext[n:]) != 1 {
		return nil, errAuthFailed
	}

	ret, plaintext := sliceForAppend(dst, n)
	cipher.XORKeyStream(plaintext, ciphertext[:n])
	return ret, nil
}

// sliceForAppend takes a slice and a requested number of bytes. It returns a
// slice with the contents of the given slice followed by that many bytes and a
// second slice that aliases into it and contains only the extra bytes. If the
// original slice has sufficient capacity then no allocation is performed.
func sliceForAppend(in []byte, n int) (head, tail []byte) {
	if total := len(in) + n; cap(in) >= total {
		head = in[:total]
	} else {
		head = make([]byte, total)
		copy(head, in)
	}
	tail = head[len(in):]
	return
}

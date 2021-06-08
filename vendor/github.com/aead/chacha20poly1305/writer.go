package chacha20poly1305

import (
	"crypto/subtle"
	"encoding/binary"
	"io"

	"github.com/aead/chacha20/chacha"
	"github.com/aead/poly1305"
)

const tagsize = poly1305.TagSize

// EncryptWriter wraps an io.Writer and returns an io.WriteCloser which
// encrypts and authenticates all input passed into it with the given key and
// nonce. The Close function of the returned io.WriteCloser must be called
// to finish the encryption successfully.
//
// If the Write or Close function of the io.WriteCloser returns a non-nil error
// the hole encryption process cannot succeed and must be restarted.
// The length of the nonce determines which cipher is used:
// 	- 8 byte: ChaCha20Poly1305 with 64 bit nonces
// 	- 12 byte: ChaCha20Poly1305 with 96 bit nonces (used in TLS)
// 	- 24 byte: XChaCha20Poly1305 with 192 bit nonces
func EncryptWriter(w io.Writer, key, nonce []byte) (io.WriteCloser, error) {
	enc, err := newAeadCipher(w, key, nonce)
	if err != nil {
		return nil, err
	}
	return &encryptedWriter{aeadCipher: *enc}, nil
}

// DecryptWriter wraps an io.Writer and returns an io.WriteCloser which
// decrypts and checks authenticity of all input passed into it with the given key
// and nonce. The Close function of the returned io.WriteCloser must be called
// to finish the decryption successfully. If the Close function returns a non-nil
// error the decryption failed - for example because of an incorrect authentication tag.
// So the returned error of Close MUST be checked!
//
// If the Write or Close function of the io.WriteCloser returns a non-nil error
// the hole decryption process cannot succeed and must be restarted.
// The length of the nonce determines which cipher is used:
// 	- 8 byte: ChaCha20Poly1305 with 64 bit nonces
// 	- 12 byte: ChaCha20Poly1305 with 96 bit nonces (used in TLS)
// 	- 24 byte: XChaCha20Poly1305 with 192 bit nonces
func DecryptWriter(w io.Writer, key, nonce []byte) (io.WriteCloser, error) {
	dec, err := newAeadCipher(w, key, nonce)
	if err != nil {
		return nil, err
	}
	return &decryptedWriter{aeadCipher: *dec}, nil
}

type aeadCipher struct {
	cipher *chacha.Cipher
	hash   *poly1305.Hash

	dst     io.Writer
	byteCtr uint64
}

func newAeadCipher(w io.Writer, key, nonce []byte) (*aeadCipher, error) {
	c, err := chacha.NewCipher(nonce, key, 20)
	if err != nil {
		return nil, err
	}
	var polyKey [32]byte
	c.XORKeyStream(polyKey[:], polyKey[:])
	c.SetCounter(1)

	return &aeadCipher{
		cipher: c,
		hash:   poly1305.New(polyKey),
		dst:    w,
	}, nil
}

func (c *aeadCipher) encrypt(p []byte) (n int, err error) {
	c.cipher.XORKeyStream(p, p)
	c.hash.Write(p)
	n, err = c.dst.Write(p)
	return
}

func (c *aeadCipher) decrypt(p []byte) (n int, err error) {
	c.hash.Write(p)
	c.cipher.XORKeyStream(p, p)
	n, err = c.dst.Write(p)
	return
}

func (c *aeadCipher) authenticate(tag *[tagsize]byte) {
	var pad [tagsize]byte
	if padCt := c.byteCtr % tagsize; padCt > 0 {
		c.hash.Write(pad[:tagsize-padCt])
	}
	binary.LittleEndian.PutUint64(pad[8:], c.byteCtr)
	c.hash.Write(pad[:])
	c.hash.Sum(tag[:0])
}

type encryptedWriter struct {
	aeadCipher
}

func (w *encryptedWriter) Write(p []byte) (n int, err error) {
	w.byteCtr += uint64(len(p))
	w.encrypt(p)
	return
}

func (w *encryptedWriter) Close() error {
	var tag [tagsize]byte
	w.authenticate(&tag)
	_, err := w.dst.Write(tag[:])
	return err
}

type decryptedWriter struct {
	aeadCipher
	buf [tagsize]byte
	off int
}

func (w *decryptedWriter) Write(p []byte) (n int, err error) {
	n = len(p)
	w.byteCtr += uint64(len(p))
	k := w.off + n
	if k < tagsize {
		w.off += copy(w.buf[w.off:], p)
		return
	}
	if w.off > 0 && k < 2*tagsize {
		var tmp [2 * tagsize]byte
		o := copy(tmp[:], w.buf[:w.off])
		o += copy(tmp[o:], p)
		i := o - tagsize
		if n, err := w.decrypt(tmp[:i]); err != nil {
			return n, err
		}
		w.off = copy(w.buf[:], tmp[i:o])
		return
	}
	if w.off > 0 {
		r := tagsize - w.off
		copy(w.buf[w.off:], p[:r])
		if n, err := w.decrypt(w.buf[:]); err != nil {
			return n, err
		}
		p = p[r:]
		w.off = 0
	}
	if nn := len(p) - tagsize; nn > 0 {
		if n, err := w.decrypt(p[:nn]); err != nil {
			return n, err
		}
		p = p[nn:]
	}
	if len(p) > 0 {
		w.off = copy(w.buf[:], p)
	}
	return
}

func (w *decryptedWriter) Close() error {
	w.byteCtr -= tagsize // we only count the ciphertext, not the tag

	var tag [tagsize]byte
	w.authenticate(&tag)
	if subtle.ConstantTimeCompare(tag[:], w.buf[:]) != 1 {
		return errAuthFailed
	}
	return nil
}

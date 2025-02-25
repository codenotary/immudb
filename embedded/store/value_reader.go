package store

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"fmt"
	"hash"
	"io"
)

type valueReader struct {
	st *Ledger

	expectedLen int
	vLogID      byte
	off         int64
	n           int
}

func (r *valueReader) Read(p []byte) (int, error) {
	readLen := len(p)
	if r.n+readLen > r.expectedLen {
		readLen = r.expectedLen - r.n
	}

	n, err := r.st.readValue(r.vLogID, r.off, p[:readLen])
	if err != nil {
		return n, err
	}

	r.n += n

	if r.n > r.expectedLen {
		panic("unexpected read length")
	}

	if r.n == r.expectedLen {
		return n, io.EOF
	}
	return n, err
}

type digestCheckReader struct {
	expectedDigest [sha256.Size]byte
	h              hash.Hash

	r io.Reader
}

func (r *digestCheckReader) Read(p []byte) (int, error) {
	n, err := r.r.Read(p)
	if err != nil && !errors.Is(err, io.EOF) {
		return n, err
	}

	if n > 0 {
		if _, err := r.h.Write(p[:n]); err != nil {
			return n, err
		}
	}

	if errors.Is(err, io.EOF) {
		if digest := r.h.Sum(nil); !bytes.Equal(digest, r.expectedDigest[:]) {
			return n, fmt.Errorf("%w: digest mismatch", ErrCorruptedData)
		}
	}
	return n, err
}

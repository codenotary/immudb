package appendable

import (
	"crypto/sha256"
	"hash"
)

type ChecksumAppendable struct {
	Appendable

	h hash.Hash
}

func WithChecksum(app Appendable) *ChecksumAppendable {
	return &ChecksumAppendable{
		Appendable: app,
		h:          sha256.New(),
	}
}

func (app *ChecksumAppendable) Append(bs []byte) (off int64, n int, err error) {
	off, n, err = app.Appendable.Append(bs)
	if err == nil {
		_, _ = app.h.Write(bs)
	}
	return off, n, err
}

func (app *ChecksumAppendable) Sum(b []byte) (checksum [sha256.Size]byte) {
	_, _ = app.h.Write(b)

	copy(checksum[:], app.h.Sum(nil))
	return checksum
}

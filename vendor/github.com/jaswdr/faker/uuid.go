package faker

import (
	"crypto/rand"
	"fmt"
	"io"
)

type UUID struct {
	Faker *Faker
}

func (u UUID) V4() (uuid string) {
	var uiq [16]byte
	io.ReadFull(rand.Reader, uiq[:])
	uiq[6] = (uiq[6] & 0x0f) | 0x40           // Version 4
	uiq[8] = (uiq[8]&(0xff>>2) | (0x02 << 6)) // Variant RFC4122
	return fmt.Sprintf("%x%x%x%x%x", uiq[0:4], uiq[4:6], uiq[6:8], uiq[8:10], uiq[10:])
}

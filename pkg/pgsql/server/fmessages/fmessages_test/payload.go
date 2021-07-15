package fmessages_test

import (
	"bytes"
	"encoding/binary"
)

func Join(chunk [][]byte) []byte {
	return bytes.Join(chunk, nil)
}

func S(str string) []byte {
	return bytes.Join([][]byte{[]byte(str), {0}}, nil)
}
func B(b []byte) []byte {
	return b
}

func I16(i int) []byte {
	ib := make([]byte, 2)
	binary.BigEndian.PutUint16(ib, uint16(i))
	return ib
}
func I32(i int) []byte {
	ib := make([]byte, 4)
	binary.BigEndian.PutUint32(ib, uint32(i))
	return ib
}

func Msg(t byte, payload []byte) []byte {
	ml := make([]byte, 4)
	binary.BigEndian.PutUint32(ml, uint32(len(payload)+4))
	return bytes.Join([][]byte{{t}, ml, payload}, nil)
}

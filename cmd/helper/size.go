package helper

import (
	"strconv"
	"strings"
)

const (
	Byte     = 1
	KiloByte = 1 << (10 * iota)
	MegaByte
	GigaByte
	TeraByte
	PetaByte
	ExaByte
)

var unitMap = map[int]string{
	Byte:     "B",
	KiloByte: "KB",
	MegaByte: "MB",
	GigaByte: "GB",
	TeraByte: "TB",
	PetaByte: "PB",
	ExaByte:  "EB",
}

func FormatByteSize(size uint64) string {
	u := getUnit(size)

	fsize := float64(size) / float64(u)
	if rounded := uint64(fsize + 0.05); rounded == 1024 {
		u = getUnit(1024 * uint64(u))
		fsize = 1
	}
	return strings.TrimSuffix(strconv.FormatFloat(fsize, 'f', 1, 64), ".0") + " " + unitMap[u]
}

func getUnit(size uint64) int {
	switch {
	case size >= ExaByte:
		return ExaByte
	case size >= PetaByte:
		return PetaByte
	case size >= TeraByte:
		return TeraByte
	case size >= GigaByte:
		return GigaByte
	case size >= MegaByte:
		return MegaByte
	case size >= KiloByte:
		return KiloByte
	}
	return Byte
}

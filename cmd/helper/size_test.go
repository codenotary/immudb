package helper_test

import (
	"testing"

	"github.com/codenotary/immudb/cmd/helper"
	"github.com/stretchr/testify/require"
)

func TestFormatByteSize(t *testing.T) {
	require.Equal(t, "25 B", helper.FormatByteSize(25))
	require.Equal(t, "10 KB", helper.FormatByteSize(size(10.0, helper.KiloByte)))
	require.Equal(t, "5.4 MB", helper.FormatByteSize(size(5.4, helper.MegaByte)))
	require.Equal(t, "11.8 GB", helper.FormatByteSize(size(11.8, helper.GigaByte)))
	require.Equal(t, "99.3 PB", helper.FormatByteSize(size(99.27, helper.PetaByte)))
	require.Equal(t, "1 EB", helper.FormatByteSize(size(1023.95, helper.PetaByte)))
	require.Equal(t, "2.3 EB", helper.FormatByteSize(size(2.3, helper.ExaByte)))
}

func size(fraction float64, unit int) uint64 {
	s := fraction * float64(unit)
	return uint64(s)
}

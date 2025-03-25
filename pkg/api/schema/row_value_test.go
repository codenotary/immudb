/*
Copyright 2025 Codenotary Inc. All rights reserved.

SPDX-License-Identifier: BUSL-1.1
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    https://mariadb.com/bsl11/

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package schema

import (
	"encoding/hex"
	"math"
	"testing"
	"time"

	"github.com/codenotary/immudb/embedded/sql"
	"github.com/stretchr/testify/require"
)

func TestRowComparison(t *testing.T) {
	nullValue := &SQLValue_Null{}
	trueValue := &SQLValue_B{B: true}
	falseValue := &SQLValue_B{B: false}
	stringValue1 := &SQLValue_S{S: "string1"}
	stringValue2 := &SQLValue_S{S: "string2"}
	intValue1 := &SQLValue_N{N: 1}
	intValue2 := &SQLValue_N{N: 2}
	blobValue1 := &SQLValue_Bs{Bs: nil}
	blobValue2 := &SQLValue_Bs{Bs: []byte{1, 2, 3}}
	tsValue1 := &SQLValue_Ts{Ts: time.Date(2021, 12, 8, 13, 46, 23, 12345000, time.UTC).UnixNano() / 1e3}
	tsValue2 := &SQLValue_Ts{Ts: time.Date(2020, 11, 7, 12, 45, 22, 12344000, time.UTC).UnixNano() / 1e3}
	float64Value1 := &SQLValue_F{F: 1.1}
	float64Value2 := &SQLValue_F{F: .1}
	float64Value3 := &SQLValue_F{F: 0.0}
	float64Value4 := &SQLValue_F{F: math.MaxFloat64}
	float64Value5 := &SQLValue_F{F: -math.MaxFloat64}
	float64Value6 := &SQLValue_F{F: -0.0}

	equals, err := nullValue.Equal(nullValue)
	require.NoError(t, err)
	require.True(t, equals)

	equals, err = nullValue.Equal(trueValue)
	require.NoError(t, err)
	require.False(t, equals)

	equals, err = trueValue.Equal(nullValue)
	require.NoError(t, err)
	require.False(t, equals)

	_, err = trueValue.Equal(stringValue1)
	require.ErrorIs(t, err, sql.ErrNotComparableValues)

	equals, err = trueValue.Equal(falseValue)
	require.NoError(t, err)
	require.False(t, equals)

	equals, err = stringValue1.Equal(nullValue)
	require.NoError(t, err)
	require.False(t, equals)

	_, err = stringValue1.Equal(trueValue)
	require.ErrorIs(t, err, sql.ErrNotComparableValues)

	equals, err = stringValue1.Equal(stringValue2)
	require.NoError(t, err)
	require.False(t, equals)

	equals, err = intValue1.Equal(nullValue)
	require.NoError(t, err)
	require.False(t, equals)

	_, err = intValue1.Equal(trueValue)
	require.ErrorIs(t, err ,sql.ErrNotComparableValues)

	equals, err = intValue1.Equal(intValue2)
	require.NoError(t, err)
	require.False(t, equals)

	equals, err = blobValue1.Equal(nullValue)
	require.NoError(t, err)
	require.False(t, equals)

	_, err = blobValue1.Equal(trueValue)
	require.ErrorIs(t, err, sql.ErrNotComparableValues)

	equals, err = blobValue1.Equal(blobValue2)
	require.NoError(t, err)
	require.False(t, equals)

	equals, err = tsValue1.Equal(tsValue1)
	require.NoError(t, err)
	require.True(t, equals)

	equals, err = tsValue1.Equal(nullValue)
	require.NoError(t, err)
	require.False(t, equals)

	equals, err = tsValue1.Equal(tsValue2)
	require.NoError(t, err)
	require.False(t, equals)

	_, err = tsValue1.Equal(stringValue1)
	require.ErrorIs(t, err, sql.ErrNotComparableValues)

	rawNilValue := RawValue(nil)
	require.Equal(t, nil, rawNilValue)

	rawNullValue := RawValue(&SQLValue{Value: nullValue})
	require.Equal(t, nil, rawNullValue)

	rawTrueValue := RawValue(&SQLValue{Value: trueValue})
	require.Equal(t, true, rawTrueValue)

	rawFalseValue := RawValue(&SQLValue{Value: falseValue})
	require.Equal(t, false, rawFalseValue)

	rawStringValue := RawValue(&SQLValue{Value: stringValue1})
	require.Equal(t, "string1", rawStringValue)

	rawIntValue := RawValue(&SQLValue{Value: intValue1})
	require.Equal(t, int64(1), rawIntValue)

	rawBlobValue := RawValue(&SQLValue{Value: blobValue2})
	require.Equal(t, []byte{1, 2, 3}, rawBlobValue)

	rawTimestampValue := RawValue(&SQLValue{Value: tsValue1})
	require.Equal(t, time.Date(2021, 12, 8, 13, 46, 23, 12345000, time.UTC), rawTimestampValue)

	nv := SQLValue{Value: nullValue}
	bytesNullValue := RenderValueAsByte(nv.GetValue())
	require.Equal(t, []byte(nil), bytesNullValue)

	tv := SQLValue{Value: trueValue}
	bytesTrueValue := RenderValueAsByte(tv.GetValue())
	require.Equal(t, []byte(`true`), bytesTrueValue)

	bf := SQLValue{Value: falseValue}
	bytesFalseValue := RenderValueAsByte(bf.GetValue())
	require.Equal(t, []byte(`false`), bytesFalseValue)

	sv := &SQLValue{Value: stringValue1}
	bytesStringValue := RenderValueAsByte(sv.GetValue())
	require.Equal(t, []byte("string1"), bytesStringValue)

	iv := &SQLValue{Value: intValue1}
	bytesIntValue := RenderValueAsByte(iv.GetValue())
	require.Equal(t, []byte(`1`), bytesIntValue)

	bv := &SQLValue{Value: blobValue2}
	bytesBlobValue := RenderValueAsByte(bv.GetValue())
	require.Equal(t, []byte(hex.EncodeToString([]byte{1, 2, 3})), bytesBlobValue)

	tsv := &SQLValue{Value: tsValue2}
	bytesTimestampValue := RenderValueAsByte(tsv.GetValue())
	require.Equal(t, []byte("2020-11-07 12:45:22.012344"), bytesTimestampValue)

	nv = SQLValue{Value: nullValue}
	rNullValue := RenderValue(nv.GetValue())
	require.Equal(t, "NULL", rNullValue)

	tv = SQLValue{Value: trueValue}
	rTrueValue := RenderValue(tv.GetValue())
	require.Equal(t, "true", rTrueValue)

	bf = SQLValue{Value: falseValue}
	rFalseValue := RenderValue(bf.GetValue())
	require.Equal(t, "false", rFalseValue)

	sv = &SQLValue{Value: stringValue1}
	rStringValue := RenderValue(sv.GetValue())
	require.Equal(t, "\"string1\"", rStringValue)

	iv = &SQLValue{Value: intValue1}
	rIntValue := RenderValue(iv.GetValue())
	require.Equal(t, "1", rIntValue)

	bv = &SQLValue{Value: blobValue2}
	rBlobValue := RenderValue(bv.GetValue())
	require.Equal(t, "010203", rBlobValue)

	tsv = &SQLValue{Value: tsValue1}
	rTimestampValue := RenderValue(tsv.GetValue())
	require.Equal(t, "2021-12-08 13:46:23.012345", rTimestampValue)

	ftv := &SQLValue{Value: float64Value1}
	floatValue := RenderValue(ftv.GetValue())
	require.Equal(t, "1.1", floatValue)
	floatValueB := RenderValueAsByte(ftv.GetValue())
	require.Equal(t, []byte("1.1"), floatValueB)
	floatValueR := RawValue(ftv)
	require.Equal(t, 1.1, floatValueR)

	ftv = &SQLValue{Value: float64Value2}
	floatValue = RenderValue(ftv.GetValue())
	require.Equal(t, "0.1", floatValue)

	ftv = &SQLValue{Value: float64Value3}
	floatValue = RenderValue(ftv.GetValue())
	require.Equal(t, "0", floatValue)

	ftv = &SQLValue{Value: float64Value4}
	floatValue = RenderValue(ftv.GetValue())
	require.Equal(t, "179769313486231570000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000", floatValue)

	ftv = &SQLValue{Value: float64Value5}
	floatValue = RenderValue(ftv.GetValue())
	require.Equal(t, "-179769313486231570000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000", floatValue)

	ftv = &SQLValue{Value: float64Value6}
	floatValue = RenderValue(ftv.GetValue())
	require.Equal(t, "0", floatValue)

	fakeSV := &SQLValue{Value: &FakeSqlValue{}}
	fakeValue := RenderValue(fakeSV.GetValue())
	require.Equal(t, "&{}", fakeValue)
	fake := RawValue(fakeSV)
	require.Equal(t, nil, fake)
	fakeB := RenderValueAsByte(fakeSV.GetValue())
	require.Equal(t, []byte(`&{}`), fakeB)
}

type FakeSqlValue struct{}

func (*FakeSqlValue) isSQLValue_Value() {}

/*
Copyright 2022 Codenotary Inc. All rights reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

	http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package sql

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestApplyImplicitConversion(t *testing.T) {
	for _, d := range []struct {
		val          TypedValue
		requiredType SQLValueType
		result       interface{}
	}{
		{&Integer{val: 1}, IntegerType, int64(1)},
		{&Integer{val: 1}, Float64Type, float64(1)},
		{&Float64{val: 1}, Float64Type, float64(1)},
		{&Varchar{val: "hello world"}, IntegerType, "hello world"},
	} {
		t.Run(fmt.Sprintf("%+v", d), func(t *testing.T) {
			converted := applyImplicitConversion(d.val, d.requiredType)
			require.Equal(t, d.result, converted)
		})
	}
}

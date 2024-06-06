/*
Copyright 2024 Codenotary Inc. All rights reserved.

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

package benchmarks

import (
	"fmt"
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestToHumanReadable(t *testing.T) {
	for _, d := range []struct {
		b uint64
		s string
	}{
		{0, "0"},
		{1, "1"},
		{999, "999"},
		{1000, "1.00k"},
		{1001, "1.00k"},
		{3333333, "3.33M"},
		{4444444444, "4.44G"},
		{5555555555555, "5.56T"},
		{6666666666666666, "6.67P"},
		{7777777777777777777, "7.78E"},
		{math.MaxUint64, "18.45E"},
	} {
		t.Run(fmt.Sprintf("%v", d), func(t *testing.T) {
			s := ToHumanReadable(d.b)
			assert.Equal(t, d.s, s)
		})
	}
}

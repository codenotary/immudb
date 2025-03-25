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

package sql

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestTimeConversions(t *testing.T) {
	for _, d := range []struct {
		t time.Time
		i int64
	}{
		{time.Date(2021, 12, 8, 13, 55, 23, 0, time.UTC), 1638971723000000},
		{time.Date(2021, 12, 8, 13, 55, 23, 123456000, time.UTC), 1638971723123456},
		{time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC), 0},
		{time.Date(0, 1, 1, 0, 0, 0, 0, time.UTC), -62167219200000000},
	} {
		t.Run(fmt.Sprintf("convert time (%v) to int64 (%d)", d.t, d.i), func(t *testing.T) {
			assert.Equal(t, d.i, TimeToInt64(d.t))
		})
		t.Run(fmt.Sprintf("convert int64 (%d) to time (%v)", d.i, d.t), func(t *testing.T) {
			assert.Equal(t, d.t, TimeFromInt64(d.i))
		})
	}
}

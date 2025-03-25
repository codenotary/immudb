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

package bmessages

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestErrorResponse(t *testing.T) {
	er := ErrorResponse(Severity("severity"),
		Code("test"),
		Message("test"),
		Hint("test"),
		SeverityNotLoc("test"),
		Detail("test"),
		Position("test"),
		InternalPosition("test"),
		InternalQuery("test"),
		Where("test"),
		SchemaName("test"),
		TableName("test"),
		ColumnName("test"),
		DataTypeName("test"),
		ConstraintName("test"),
		File("test"),
		Line("test"),
		Routine("test"),
	)
	require.NotNil(t, er)
}

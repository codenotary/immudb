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

package sql

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestConditionalRowReader(t *testing.T) {
	dummyr := &dummyRowReader{failReturningColumns: true}

	rowReader := newConditionalRowReader(dummyr, &Bool{val: true})

	_, err := rowReader.Columns(context.Background())
	require.ErrorIs(t, err, errDummy)

	err = rowReader.InferParameters(context.Background(), nil)
	require.ErrorIs(t, err, errDummy)

	dummyr.failInferringParams = true

	err = rowReader.InferParameters(context.Background(), nil)
	require.ErrorIs(t, err, errDummy)
}

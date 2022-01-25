/*
Copyright 2022 CodeNotary, Inc. All rights reserved.

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
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLimitRowReader(t *testing.T) {
	dummyr := &dummyRowReader{failReturningColumns: false}

	rowReader, err := newLimitRowReader(dummyr, 1)
	require.NoError(t, err)

	require.Equal(t, dummyr.Database(), rowReader.Database())
	require.Equal(t, dummyr.TableAlias(), rowReader.TableAlias())
	require.Equal(t, dummyr.OrderBy(), rowReader.OrderBy())
	require.Equal(t, dummyr.ScanSpecs(), rowReader.ScanSpecs())

	require.Nil(t, rowReader.Tx())

	_, err = rowReader.Read()
	require.Equal(t, errDummy, err)

	dummyr.failReturningColumns = true
	_, err = rowReader.Columns()
	require.Equal(t, errDummy, err)

	require.Nil(t, rowReader.Parameters())

	err = rowReader.InferParameters(nil)
	require.NoError(t, err)

	dummyr.failInferringParams = true

	err = rowReader.InferParameters(nil)
	require.Equal(t, errDummy, err)
}

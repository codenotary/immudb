/*
Copyright 2021 CodeNotary, Inc. All rights reserved.

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

func TestEmptyCatalog(t *testing.T) {
	catalog := &Catalog{}

	dbs := catalog.Databases()
	require.Empty(t, dbs)

	exists := catalog.ExistDatabase("db1")
	require.False(t, exists)
}

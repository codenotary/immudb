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

package schema

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestPattern_Pattern_ImmuService_VerifiableGet_0(t *testing.T) {
	p := Pattern_ImmuService_VerifiableGet_0()
	require.NotNil(t, p)
}

func TestPattern_ImmuService_VerifiableSet_0(t *testing.T) {
	p := Pattern_ImmuService_VerifiableSet_0()
	require.NotNil(t, p)
}

func TestPattern_ImmuService_Set_0(t *testing.T) {
	p := Pattern_ImmuService_Set_0()
	require.NotNil(t, p)
}

func TestPattern_ImmuService_History_0(t *testing.T) {
	p := Pattern_ImmuService_History_0()
	require.NotNil(t, p)
}

func TestPattern_ImmuService_VerifiableSetReference_0(t *testing.T) {
	p := Pattern_ImmuService_VerifiableSetReference_0()
	require.NotNil(t, p)
}

func TestPattern_ImmuService_VerifiableZAdd_0(t *testing.T) {
	p := Pattern_ImmuService_VerifiableZAdd_0()
	require.NotNil(t, p)
}

func TestPattern_ImmuService_UseDatabase_0(t *testing.T) {
	p := Pattern_ImmuService_UseDatabase_0()
	require.NotNil(t, p)
}

func TestPattern_ImmuService_VerifiableTxById_0(t *testing.T) {
	p := Pattern_ImmuService_VerifiableTxById_0()
	require.NotNil(t, p)
}

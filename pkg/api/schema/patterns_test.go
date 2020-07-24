/*
Copyright 2019-2020 vChain, Inc.

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
	"github.com/grpc-ecosystem/grpc-gateway/runtime"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestPattern_ImmuService_SafeGet_0(t *testing.T) {
	p := Pattern_ImmuService_SafeGet_0()
	assert.IsType(t, runtime.Pattern{}, p)
}

func TestPattern_ImmuService_SafeReference_0(t *testing.T) {
	p := Pattern_ImmuService_SafeReference_0()
	assert.IsType(t, runtime.Pattern{}, p)
}

func TestPattern_ImmuService_SafeZAdd_0(t *testing.T) {
	p := Pattern_ImmuService_SafeZAdd_0()
	assert.IsType(t, runtime.Pattern{}, p)
}
func TestPattern_ImmuService_Set_0(t *testing.T) {
	p := Pattern_ImmuService_Set_0()
	assert.IsType(t, runtime.Pattern{}, p)
}
func TestPattern_ImmuService_SafeSet_0(t *testing.T) {
	p := Pattern_ImmuService_SafeSet_0()
	assert.IsType(t, runtime.Pattern{}, p)
}

func TestPattern_ImmuService_History_0(t *testing.T) {
	p := Pattern_ImmuService_History_0()
	assert.IsType(t, runtime.Pattern{}, p)
}

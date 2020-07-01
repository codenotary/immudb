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

package client

import (
	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/stretchr/testify/assert"
	"io"
	"strings"
	"testing"
)

func TestBatchRequest(t *testing.T) {
	br := BatchRequest{
		Keys:   []io.Reader{strings.NewReader("key1"), strings.NewReader("key2")},
		Values: []io.Reader{strings.NewReader("val1"), strings.NewReader("val2")},
	}
	kvl, err := br.toKVList()
	assert.Nil(t, err)
	assert.IsType(t, kvl, &schema.KVList{})
}

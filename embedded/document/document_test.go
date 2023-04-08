/*
Copyright 2023 Codenotary Inc. All rights reserved.

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
package document

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func newDoc(id, name, cid interface{}) *Document {
	doc, err := NewDocumentFrom(map[string]interface{}{
		"id":   id,
		"name": name,
		"age":  cid,
	})
	if err != nil {
		panic(err)
	}
	return doc
}

func TestDocument(t *testing.T) {
	type user struct {
		ID   string `json:"id"`
		Name string `json:"name"`
		Age  int    `json:"age"`
	}
	usr := user{
		ID:   "1",
		Name: "foo",
		Age:  10,
	}
	r, err := NewDocumentFrom(&usr)
	if err != nil {
		t.Fatal(err)
	}
	t.Run("get", func(t *testing.T) {
		usr := newDoc(2, "bar", 3)
		assert.Equal(t, "bar", usr.Get("name"))
		assert.Equal(t, 3.0, usr.Get("age"))
	})
	t.Run("bytes", func(t *testing.T) {
		assert.NotEmpty(t, string(r.Bytes()))
	})
	t.Run("new from bytes", func(t *testing.T) {
		n, err := NewDocumentFromBytes(r.Bytes())
		assert.NoError(t, err)
		assert.Equal(t, true, n.Valid())
	})
	t.Run("unmarshalJSON", func(t *testing.T) {
		usr := newDoc(7, "baz", 4)
		bits, err := usr.MarshalJSON()
		assert.NoError(t, err)
		usr2 := NewDocument()
		assert.NoError(t, usr2.UnmarshalJSON(bits))
		assert.Equal(t, usr.String(), usr2.String())
	})
}

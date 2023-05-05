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
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/structpb"
)

func newDoc(id float64, name string, age float64) *Document {
	doc, err := NewDocumentFrom(&structpb.Struct{
		Fields: map[string]*structpb.Value{
			"id":   structpb.NewNumberValue(id),
			"name": structpb.NewStringValue(name),
			"age":  structpb.NewNumberValue(age),
		},
	})
	if err != nil {
		panic(err)
	}
	return doc
}

func TestDocument(t *testing.T) {
	r, err := NewDocumentFrom(&structpb.Struct{
		Fields: map[string]*structpb.Value{
			"name": structpb.NewStringValue("foo"),
			"id":   structpb.NewNumberValue(1),
			"age":  structpb.NewNumberValue(10),
		},
	})
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

	t.Run("check field ordering", func(t *testing.T) {
		r1, _ := NewDocumentFrom(&structpb.Struct{
			Fields: map[string]*structpb.Value{
				"name": structpb.NewStringValue("foo"),
				"id":   structpb.NewNumberValue(1),
				"age":  structpb.NewNumberValue(10),
			},
		})
		r2, _ := NewDocumentFrom(&structpb.Struct{
			Fields: map[string]*structpb.Value{
				"age":  structpb.NewNumberValue(10),
				"id":   structpb.NewNumberValue(1),
				"name": structpb.NewStringValue("foo"),
			},
		})
		require.Equal(t, r1.Bytes(), r2.Bytes())
	})
}

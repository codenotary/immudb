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
			"id":   {Kind: &structpb.Value_NumberValue{NumberValue: id}},
			"name": {Kind: &structpb.Value_StringValue{StringValue: name}},
			"age":  {Kind: &structpb.Value_NumberValue{NumberValue: age}},
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
			"name": {Kind: &structpb.Value_StringValue{StringValue: "foo"}},
			"id":   {Kind: &structpb.Value_NumberValue{NumberValue: 1}},
			"age":  {Kind: &structpb.Value_NumberValue{NumberValue: 10}},
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
				"name": {Kind: &structpb.Value_StringValue{StringValue: "foo"}},
				"id":   {Kind: &structpb.Value_NumberValue{NumberValue: 1}},
				"age":  {Kind: &structpb.Value_NumberValue{NumberValue: 10}},
			},
		})
		r2, _ := NewDocumentFrom(&structpb.Struct{
			Fields: map[string]*structpb.Value{
				"age":  {Kind: &structpb.Value_NumberValue{NumberValue: 10}},
				"id":   {Kind: &structpb.Value_NumberValue{NumberValue: 1}},
				"name": {Kind: &structpb.Value_StringValue{StringValue: "foo"}},
			},
		})
		require.Equal(t, r1.Bytes(), r2.Bytes())
	})
}

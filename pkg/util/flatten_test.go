package util

import (
	"encoding/json"
	"reflect"
	"testing"
)

func TestFlatten(t *testing.T) {
	tests := []struct {
		input  string
		option *Option
		want   map[string]interface{}
	}{
		{
			`{"foo": "bar"}`,
			nil,
			map[string]interface{}{"foo": "bar"},
		},
		{
			`{"foo": true}`,
			nil,
			map[string]interface{}{"foo": true},
		},
		{
			`{"foo": null}`,
			nil,
			map[string]interface{}{"foo": nil},
		},
		// nested once
		{
			`{"foo":{}}`,
			nil,
			map[string]interface{}{"foo": map[string]interface{}{}},
		},
		{
			`{"foo":{"bar":"baz"}}`,
			nil,
			map[string]interface{}{"foo.bar": "baz"},
		},
		{
			`{"foo":{"bar":true}}`,
			nil,
			map[string]interface{}{"foo.bar": true},
		},
		{
			`{"foo":{"bar":null}}`,
			nil,
			map[string]interface{}{"foo.bar": nil},
		},
		// slice
		{
			`{"foo":{"bar":["one","two"]}}`,
			nil,
			map[string]interface{}{
				"foo.bar.0": "one",
				"foo.bar.1": "two",
			},
		},
		// custom delimiter
		{
			`{"foo":{"bar":{"again":"baz"}}}`,
			&Option{
				Delimiter: "-",
			},
			map[string]interface{}{"foo-bar-again": "baz"},
		},
	}
	for i, test := range tests {
		var input interface{}
		err := json.Unmarshal([]byte(test.input), &input)
		if err != nil {
			t.Errorf("%d: failed to unmarshal test: %v", i+1, err)
		}
		got := Flatten(input.(map[string]interface{}), test.option)
		if !reflect.DeepEqual(got, test.want) {
			t.Errorf("%d: mismatch, got: %v want: %v", i+1, got, test.want)
		}
	}
}

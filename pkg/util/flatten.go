package util

import (
	"reflect"
	"strconv"
)

type Option struct {
	Prefix    string
	Delimiter string
	MaxDepth  int
}

func DefaultOption() *Option {
	return &Option{
		Delimiter: ".",
	}
}

// Flatten takes a nested map and returns a flattened map.
func Flatten(nested map[string]interface{}, opts *Option) (m map[string]interface{}) {
	if opts == nil {
		opts = DefaultOption()
	}

	return flatten(opts.Prefix, 0, nested, opts)
}

func flatten(prefix string, depth int, nested interface{}, opts *Option) (res map[string]interface{}) {
	res = make(map[string]interface{})

	switch t := nested.(type) {
	case map[string]interface{}:
		if opts.MaxDepth != 0 && depth >= opts.MaxDepth {
			res[prefix] = t
			return
		}
		if reflect.DeepEqual(t, map[string]interface{}{}) {
			res[prefix] = t
			return
		}
		for k, v := range t {
			key := withPrefix(prefix, k, opts)
			fm1 := flatten(key, depth+1, v, opts)
			mergeMap(res, fm1)
		}
	case []interface{}:
		if reflect.DeepEqual(t, []interface{}{}) {
			res[prefix] = t
			return
		}
		for i, v := range t {
			key := withPrefix(prefix, strconv.Itoa(i), opts)
			fm1 := flatten(key, depth+1, v, opts)
			mergeMap(res, fm1)
		}
	default:
		res[prefix] = t
	}
	return
}

func withPrefix(prefix string, key string, opts *Option) string {
	if prefix != "" {
		return prefix + opts.Delimiter + key
	}
	return key
}

func mergeMap(to map[string]interface{}, from map[string]interface{}) {
	for kt, vt := range from {
		to[kt] = vt
	}
}

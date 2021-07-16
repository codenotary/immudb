package schema

import (
	"github.com/codenotary/immudb/embedded/sql"
)

func EncodeParams(params map[string]interface{}) ([]*NamedParam, error) {
	if params == nil {
		return nil, nil
	}

	namedParams := make([]*NamedParam, len(params))

	i := 0
	for n, v := range params {
		sqlVal, err := asSQLValue(v)
		if err != nil {
			return nil, err
		}

		namedParams[i] = &NamedParam{Name: n, Value: sqlVal}
		i++
	}

	return namedParams, nil
}

func asSQLValue(v interface{}) (*SQLValue, error) {
	if v == nil {
		return &SQLValue{Value: &SQLValue_Null{}}, nil
	}

	switch tv := v.(type) {
	case uint:
		{
			return &SQLValue{Value: &SQLValue_N{N: uint64(tv)}}, nil
		}
	case int:
		{
			return &SQLValue{Value: &SQLValue_N{N: uint64(tv)}}, nil
		}
	case int64:
		{
			return &SQLValue{Value: &SQLValue_N{N: uint64(tv)}}, nil
		}
	case uint64:
		{
			return &SQLValue{Value: &SQLValue_N{N: uint64(tv)}}, nil
		}
	case string:
		{
			return &SQLValue{Value: &SQLValue_S{S: tv}}, nil
		}
	case bool:
		{
			return &SQLValue{Value: &SQLValue_B{B: tv}}, nil
		}
	case []byte:
		{
			return &SQLValue{Value: &SQLValue_Bs{Bs: tv}}, nil
		}
	}

	return nil, sql.ErrInvalidValue
}

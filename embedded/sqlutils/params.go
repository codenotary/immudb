package sqlutils

import (
	"github.com/codenotary/immudb/embedded/sql"
	"github.com/codenotary/immudb/pkg/api/schema"
)

func EncodeParams(params map[string]interface{}) ([]*schema.NamedParam, error) {
	if params == nil {
		return nil, nil
	}

	namedParams := make([]*schema.NamedParam, len(params))

	i := 0
	for n, v := range params {
		sqlVal, err := asSQLValue(v)
		if err != nil {
			return nil, err
		}

		namedParams[i] = &schema.NamedParam{Name: n, Value: sqlVal}
		i++
	}

	return namedParams, nil
}

func asSQLValue(v interface{}) (*schema.SQLValue, error) {
	if v == nil {
		return &schema.SQLValue{Value: &schema.SQLValue_Null{}}, nil
	}

	switch tv := v.(type) {
	case uint:
		{
			return &schema.SQLValue{Value: &schema.SQLValue_N{N: uint64(tv)}}, nil
		}
	case int:
		{
			return &schema.SQLValue{Value: &schema.SQLValue_N{N: uint64(tv)}}, nil
		}
	case int64:
		{
			return &schema.SQLValue{Value: &schema.SQLValue_N{N: uint64(tv)}}, nil
		}
	case uint64:
		{
			return &schema.SQLValue{Value: &schema.SQLValue_N{N: uint64(tv)}}, nil
		}
	case string:
		{
			return &schema.SQLValue{Value: &schema.SQLValue_S{S: tv}}, nil
		}
	case bool:
		{
			return &schema.SQLValue{Value: &schema.SQLValue_B{B: tv}}, nil
		}
	case []byte:
		{
			return &schema.SQLValue{Value: &schema.SQLValue_Bs{Bs: tv}}, nil
		}
	}

	return nil, sql.ErrInvalidValue
}

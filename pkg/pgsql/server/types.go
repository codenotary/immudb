/*
Copyright 2021 CodeNotary, Inc. All rights reserved.

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

package server

import (
	"encoding/binary"
	"fmt"
	"github.com/codenotary/immudb/embedded/sql"
	"github.com/codenotary/immudb/pkg/api/schema"
	"strconv"
)

func buildNamedParams(paramsType []*schema.Column, paramsVal []interface{}) ([]*schema.NamedParam, error) {
	pMap := make(map[string]interface{})
	for index, param := range paramsType {
		val := paramsVal[index]
		// text param
		if p, ok := val.(string); ok {
			switch param.Type {
			case "INTEGER":
				int, err := strconv.Atoi(p)
				if err != nil {
					return nil, err
				}
				pMap[param.Name] = int64(int)
			case "VARCHAR":
				pMap[param.Name] = p
			case "BOOLEAN":
				pMap[param.Name] = p
			case "BLOB":
				pMap[param.Name] = p
			}
		}
		// binary param
		if p, ok := val.([]byte); ok {
			switch param.Type {
			case "INTEGER":
				i, err := getInt64(p)
				if err != nil {
					return nil, err
				}
				pMap[param.Name] = i
			case "VARCHAR":
				pMap[param.Name] = string(p)
			case "BOOLEAN":
				v := false
				if p[0] == byte(1) {
					v = true
				}
				pMap[param.Name] = v
			case "BLOB":
				pMap[param.Name] = p
			}
		}
	}

	return encodeParams(pMap)
}

func getInt64(p []byte) (int64, error) {
	switch len(p) {
	case 8:
		return int64(binary.BigEndian.Uint64(p)), nil
	case 4:
		return int64(binary.BigEndian.Uint32(p)), nil
	case 2:
		return int64(binary.BigEndian.Uint16(p)), nil
	default:
		return 0, fmt.Errorf("cannot convert a slice of %d byte in an INTEGER parameter", len(p))
	}
}

func encodeParams(params map[string]interface{}) ([]*schema.NamedParam, error) {
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

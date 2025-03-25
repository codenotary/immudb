/*
Copyright 2025 Codenotary Inc. All rights reserved.

SPDX-License-Identifier: BUSL-1.1
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    https://mariadb.com/bsl11/

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package fmessages

import (
	"bufio"
	"bytes"

	pgserrors "github.com/codenotary/immudb/pkg/pgsql/errors"
	"github.com/codenotary/immudb/pkg/pgsql/server/pgmeta"
)

// BindMsg Once a prepared statement exists, it can be readied for execution using a Bind message. The Bind message gives the
// name of the source prepared statement (empty string denotes the unnamed prepared statement), the name of the destination
// portal (empty string denotes the unnamed portal), and the values to use for any parameter placeholders present in the
// prepared statement. The supplied parameter set must match those needed by the prepared statement. (If you declared
// any void parameters in the Parse message, pass NULL values for them in the Bind message.) Bind also specifies
// the format to use for any data returned by the query; the format can be specified overall, or per-column.
// The response is either BindComplete or ErrorResponse.
//
// Note
// The choice between text and binary output is determined by the format codes given in Bind, regardless of the SQL
// command involved. The BINARY attribute in cursor declarations is irrelevant when using extended query protocol.
type BindMsg struct {
	// The name of the destination portal (an empty string selects the unnamed portal).
	DestPortalName string
	// The name of the source prepared statement (an empty string selects the unnamed prepared statement).
	PreparedStatementName string
	// The parameter format codes. Each must presently be zero (text) or one (binary).
	ParameterFormatCodes []int16
	// Array of the values of the parameters, in the format indicated by the associated format code. n is the above length.
	ParamVals []interface{}
	// The result-column format codes. Each must presently be zero (text) or one (binary).
	ResultColumnFormatCodes []int16
}

func ParseBindMsg(payload []byte) (BindMsg, error) {
	b := bytes.NewBuffer(payload)
	r := bufio.NewReaderSize(b, len(payload))
	destPortalName, err := getNextString(r)
	if err != nil {
		return BindMsg{}, err
	}
	preparedStatement, err := getNextString(r)
	if err != nil {
		return BindMsg{}, err
	}
	// The number of parameter format codes that follow (denoted C below).
	// This can be zero to indicate that there are no parameters or that the parameters all use the default format (text);
	// or one, in which case the specified format code is applied to all parameters; or it can equal the actual number
	// of parameters.
	parameterFormatCodeNumber, err := getNextInt16(r)
	if err != nil {
		return BindMsg{}, err
	}
	if parameterFormatCodeNumber < 0 {
		return BindMsg{}, pgserrors.ErrMalformedMessage
	}
	parameterFormatCodes := make([]int16, parameterFormatCodeNumber)
	for k := 0; k < int(parameterFormatCodeNumber); k++ {
		p, err := getNextInt16(r)
		if err != nil {
			return BindMsg{}, err
		}
		parameterFormatCodes[k] = p
	}
	// The number of parameter values that follow (possibly zero). This must match the number of parameters needed by the query.
	pCount, err := getNextInt16(r)
	if err != nil {
		return BindMsg{}, err
	}

	// Handling format codes: see resultColumnFormatCodesNumber property comment
	forceTXT := false
	forceBIN := false
	if len(parameterFormatCodes) == 0 {
		forceTXT = true
	}
	if len(parameterFormatCodes) == 1 {
		switch parameterFormatCodes[0] {
		case 0:
			forceTXT = true
		case 1:
			forceBIN = true
		default:
			return BindMsg{}, pgserrors.ErrMalformedMessage
		}
	}

	if len(parameterFormatCodes) > 1 && len(parameterFormatCodes) != int(pCount) {
		return BindMsg{}, pgserrors.ErrMalformedMessage
	}
	totalParamLen := 0
	params := make([]interface{}, 0)
	for i := 0; i < int(pCount); i++ {
		pLen, err := getNextInt32(r)
		if pLen < 0 {
			return BindMsg{}, pgserrors.ErrNegativeParameterValueLen
		}
		if err != nil {
			return BindMsg{}, err
		}
		totalParamLen += int(pLen)
		if totalParamLen > pgmeta.MaxMsgSize {
			return BindMsg{}, pgserrors.ErrParametersValueSizeTooLarge
		}
		pVal := make([]byte, pLen)
		_, err = r.Read(pVal)
		if err != nil {
			return BindMsg{}, err
		}
		if forceTXT {
			params = append(params, string(pVal))
			continue
		}
		if forceBIN {
			params = append(params, pVal)
			continue
		}

		switch parameterFormatCodes[i] {
		case 0:
			params = append(params, string(pVal))
		case 1:
			params = append(params, pVal)
		default:
			return BindMsg{}, pgserrors.ErrMalformedMessage
		}
	}
	// The number of result-column format codes that follow (denoted R below).
	// This can be zero to indicate that there are no result columns or that the result columns should all use the
	// default format (text); or one, in which case the specified format code is applied to all result columns (if any);
	// or it can equal the actual number of result columns of the query.
	resultColumnFormatCodesNumber, err := getNextInt16(r)
	if err != nil {
		return BindMsg{}, err
	}
	if resultColumnFormatCodesNumber < 0 {
		return BindMsg{}, pgserrors.ErrMalformedMessage
	}

	resultColumnFormatCodes := make([]int16, 0, resultColumnFormatCodesNumber)
	for k := resultColumnFormatCodesNumber; k > 0; k-- {
		p, err := getNextInt16(r)
		if err != nil {
			return BindMsg{}, err
		}
		resultColumnFormatCodes = append(resultColumnFormatCodes, p)
	}

	return BindMsg{
		DestPortalName:          destPortalName,
		PreparedStatementName:   preparedStatement,
		ParamVals:               params,
		ResultColumnFormatCodes: resultColumnFormatCodes,
	}, nil
}

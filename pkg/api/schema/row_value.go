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
package schema

import (
	"encoding/hex"
	"fmt"
	"strconv"
)

func RenderValue(op isRowValue_Operation) string {
	switch v := op.(type) {
	case *RowValue_N:
		{
			return strconv.FormatInt(int64(v.N), 10)
		}
	case *RowValue_S:
		{
			return v.S
		}
	case *RowValue_V:
		{
			return strconv.FormatBool(v.V)
		}
	case *RowValue_B:
		{
			return hex.EncodeToString(v.B)
		}
	}

	return fmt.Sprintf("%v", op)
}

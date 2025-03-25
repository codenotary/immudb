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

package models

type SearchPayload struct {
	Query    Query `json:"query"`
	Page     int   `json:"page"`
	PageSize int   `json:"pageSize"`
	KeepOpen bool  `json:"keepOpen"`
}

type Query struct {
	Expressions []Expressions `json:"expressions"`
	OrderBy     []OrderBy     `json:"orderBy"`
	Limit       int           `json:"limit"`
}

type Expressions struct {
	FieldComparisons []FieldComparison `json:"fieldComparisons"`
}

type OrderBy struct {
	Field string `json:"field"`
	Desc  bool   `json:"desc"`
}

type FieldComparison struct {
	Field    string      `json:"field"`
	Operator string      `json:"operator"`
	Value    interface{} `json:"value"`
}

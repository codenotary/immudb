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

type Collection struct {
	Name                string  `json:"name"`
	DocumentIdFieldName string  `json:"documentIdFieldName"`
	Fields              []Field `json:"fields"`
	Indexes             []Index `json:"indexes"`
}

type Field struct {
	Name string `json:"name"`
	Type string `json:"type"`
}

type Index struct {
	Fields   []string `json:"fields"`
	IsUnique bool     `json:"isUnique"`
}

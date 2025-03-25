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
package sql

import (
	"context"
)

type dummyDataSource struct {
	inferParametersFunc func(ctx context.Context, tx *SQLTx, params map[string]SQLValueType) error
	ResolveFunc         func(ctx context.Context, tx *SQLTx, params map[string]interface{}, ScanSpecs *ScanSpecs) (RowReader, error)
	AliasFunc           func() string
}

func (d *dummyDataSource) readOnly() bool {
	return true
}

func (d *dummyDataSource) requiredPrivileges() []SQLPrivilege {
	return []SQLPrivilege{SQLPrivilegeSelect}
}

func (d *dummyDataSource) execAt(ctx context.Context, tx *SQLTx, params map[string]interface{}) (*SQLTx, error) {
	return tx, nil
}

func (d *dummyDataSource) inferParameters(ctx context.Context, tx *SQLTx, params map[string]SQLValueType) error {
	return d.inferParametersFunc(ctx, tx, params)
}

func (d *dummyDataSource) Resolve(ctx context.Context, tx *SQLTx, params map[string]interface{}, scanSpecs *ScanSpecs) (RowReader, error) {
	return d.ResolveFunc(ctx, tx, params, scanSpecs)
}

func (d *dummyDataSource) Alias() string {
	return d.AliasFunc()
}

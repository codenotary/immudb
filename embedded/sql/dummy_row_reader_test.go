/*
Copyright 2024 Codenotary Inc. All rights reserved.

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
	"errors"
)

var errDummy = errors.New("dummy error")

type dummyRowReader struct {
	failReturningColumns bool
	failInferringParams  bool
	database             string
	params               map[string]interface{}

	recordClose                bool
	closed                     bool
	failSecondReturningColumns bool
}

func (r *dummyRowReader) onClose(callback func()) {
}

func (r *dummyRowReader) Tx() *SQLTx {
	return nil
}

func (r *dummyRowReader) Database() string {
	return r.database
}

func (r *dummyRowReader) TableAlias() string {
	return "table1"
}

func (r *dummyRowReader) Read(ctx context.Context) (*Row, error) {
	return nil, errDummy
}

func (r *dummyRowReader) Close() error {
	if r.recordClose {
		if r.closed {
			return ErrAlreadyClosed
		}
		r.closed = true
		return nil
	}
	return errDummy
}

func (r *dummyRowReader) OrderBy() []ColDescriptor {
	return nil
}

func (r *dummyRowReader) ScanSpecs() *ScanSpecs {
	return nil
}

func (r *dummyRowReader) Columns(ctx context.Context) ([]ColDescriptor, error) {
	if r.failReturningColumns {
		return nil, errDummy
	}

	if r.failSecondReturningColumns {
		// Will fail the next time
		r.failReturningColumns = true
	}

	return nil, nil
}

func (r *dummyRowReader) Parameters() map[string]interface{} {
	return r.params
}

func (r *dummyRowReader) InferParameters(ctx context.Context, params map[string]SQLValueType) error {
	if r.failInferringParams {
		return errDummy
	}

	return nil
}

func (r *dummyRowReader) colsBySelector(ctx context.Context) (map[string]ColDescriptor, error) {
	return nil, errDummy
}

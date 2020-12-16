/*
Copyright 2019-2020 vChain, Inc.

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

package database

import (
	"fmt"

	"github.com/codenotary/immudb/embedded/store"
	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/codenotary/immudb/pkg/common"
)

//Reference ...
func (d *db) SetReference(req *schema.ReferenceRequest) (*schema.TxMetadata, error) {
	d.mutex.Lock()
	defer d.mutex.Unlock()
	// TODO: use tx pool

	if req == nil {
		return nil, store.ErrIllegalArguments
	}
	if req.Key == nil {
		return nil, ErrReferenceKeyMissing
	}

	refVal, err := d.getReferenceVal(req, false, d.tx1)
	if err != nil {
		return nil, err
	}

	meta, err := d.st.Commit([]*store.KV{{Key: req.Reference, Value: refVal}})
	if err != nil {
		return nil, err
	}

	return schema.TxMetatadaTo(meta), err
}

//SafeReference ...
func (d *db) VerifiableSetReference(req *schema.VerifiableReferenceRequest) (*schema.VerifiableTx, error) {
	//return d.st.SafeReference(*safeRefOpts)
	return nil, fmt.Errorf("Functionality not yet supported: %s", "SafeReference")
}

func (d *db) getReferenceVal(req *schema.ReferenceRequest, skipPersistenceCheck bool, tx *store.Tx) (v []byte, err error) {
	if !skipPersistenceCheck {
		// check if key exists
		if _, err := d.getAt(req.Key, req.AtTx, 0, d.st, tx); err != nil {
			return nil, err
		}
	}

	v = common.WrapReferenceAt(req.Key, req.AtTx)
	v = common.WrapPrefix(v, common.ReferencePrefix)

	return v, err
}

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
	"bytes"
	"fmt"

	"github.com/codenotary/immudb/embedded/store"
	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/codenotary/immudb/pkg/common"
)

//Reference ...
func (d *db) SetReference(refOpts *schema.Reference) (*schema.TxMetadata, error) {
	if refOpts == nil {
		return nil, store.ErrIllegalArguments
	}
	if refOpts.Key == nil {
		return nil, ErrReferenceKeyMissing
	}

	k, err := d.getReferenceVal(refOpts, false)
	if err != nil {
		return nil, fmt.Errorf("unexpected error %v during %s", err, "Reference")
	}

	meta, err := d.st.Commit([]*store.KV{{Key: refOpts.Reference, Value: k}})
	if err != nil {
		return nil, fmt.Errorf("unexpected error %v during %s", err, "Reference")
	}

	return schema.TxMetatadaTo(meta), err
}

//SafeReference ...
func (d *db) VerifiableSetReference(req *schema.VerifiableReferenceRequest) (*schema.VerifiableTx, error) {
	//return d.st.SafeReference(*safeRefOpts)
	return nil, fmt.Errorf("Functionality not yet supported: %s", "SafeReference")
}

func (d *db) getReferenceVal(rOpts *schema.Reference, skipPersistenceCheck bool) (v []byte, err error) {
	var atTx uint64
	var key []byte

	if rOpts.AtTx > 0 {
		if !skipPersistenceCheck {
			if err := d.st.ReadTx(uint64(rOpts.AtTx), d.tx1); err != nil {
				return nil, err
			}

			// check if specific key exists at the referenced at tx
			if _, err := d.st.ReadValue(d.tx1, rOpts.Key); err != nil {
				return nil, ErrIndexKeyMismatch
			}
		}

		key = rOpts.Key
		// append the index to the reference. In this way the resolution will be at tx
		atTx = uint64(rOpts.AtTx)
	} else {
		i, err := d.Get(&schema.KeyRequest{Key: rOpts.Key})
		if err != nil {
			return nil, err
		}

		if bytes.Compare(i.Key, rOpts.Key) != 0 {
			return nil, ErrIndexKeyMismatch
		}

		key = rOpts.Key
		// atTx has not to be stored inside the reference if not submitted by the client. This is needed to permit verifications in SDKs
		atTx = 0
	}

	// append the timestamp to the reference key. In this way equal keys will be returned sorted by timestamp and the resolution will be index based
	v = common.WrapReferenceAt(key, atTx)
	v = common.WrapPrefix(v, common.ReferencePrefix)

	return v, err
}

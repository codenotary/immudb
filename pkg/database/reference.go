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
func (d *db) Reference(refOpts *schema.ReferenceOptions) (index *schema.Root, err error) {
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
	id, _, alh, err := d.st.Commit([]*store.KV{{Key: refOpts.Reference, Value: k}})
	if err != nil {
		return nil, fmt.Errorf("unexpected error %v during %s", err, "Reference")
	}
	return &schema.Root{
		Payload: &schema.RootIndex{
			Index: id,
			Root:  alh[:],
		},
	}, nil
}

//GetReference ...
func (d *db) GetReference(k *schema.Key) (*schema.Item, error) {
	if k == nil {
		return nil, store.ErrIllegalArguments
	}
	item, err := d.GetSince(k, 0)
	if err != nil {
		return nil, err
	}
	if !bytes.HasPrefix(item.Value, common.ReferencePrefix) {
		return nil, ErrNoReferenceProvided
	}
	ref := bytes.TrimPrefix(item.Value, common.ReferencePrefix)

	key, flag, refIndex := common.UnwrapIndexReference(ref)
	if flag == byte(1) {
		if err = d.st.ReadTx(refIndex, d.tx); err != nil {
			return nil, err
		}
		val, err := d.st.ReadValue(d.tx, key)
		if err != nil {
			return nil, err
		}
		return &schema.Item{Key: key, Value: val, Index: refIndex}, nil
	} else {
		return d.Get(&schema.Key{Key: key})
	}
}

//SafeReference ...
func (d *db) SafeReference(safeRefOpts *schema.SafeReferenceOptions) (proof *schema.Proof, err error) {
	//return d.st.SafeReference(*safeRefOpts)
	return nil, fmt.Errorf("Functionality not yet supported: %s", "SafeReference")
}

func (d *db) getReferenceVal(rOpts *schema.ReferenceOptions, skipPersistenceCheck bool) (v []byte, err error) {
	var index = &schema.Index{}
	var key []byte
	if rOpts.Index != nil {
		if !skipPersistenceCheck {
			if err := d.st.ReadTx(rOpts.Index.Index, d.tx); err != nil {
				return nil, err
			}
			// check if specific key exists at the referenced index
			if _, err := d.st.ReadValue(d.tx, rOpts.Key); err != nil {
				return nil, ErrIndexKeyMismatch
			}
		}
		key = rOpts.Key
		// append the index to the reference. In this way the resolution will be index based
		index = rOpts.Index
	} else {
		i, err := d.Get(&schema.Key{Key: rOpts.Key})
		if err != nil {
			return nil, err
		}
		if bytes.Compare(i.Key, rOpts.Key) != 0 {
			return nil, ErrIndexKeyMismatch
		}
		key = rOpts.Key
		// Index has not to be stored inside the reference if not submitted by the client. This is needed to permit verifications in SDKs
		index = nil
	}

	// append the timestamp to the reference key. In this way equal keys will be returned sorted by timestamp and the resolution will be index based
	v = common.WrapIndexReference(key, index)

	v = common.WrapPrefix(v, common.ReferencePrefix)

	return v, err
}

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
	"encoding/binary"
	"fmt"

	"github.com/codenotary/immudb/embedded/store"
	"github.com/codenotary/immudb/pkg/api/schema"
)

//Reference ...
func (d *db) SetReference(req *schema.ReferenceRequest) (*schema.TxMetadata, error) {
	d.mutex.Lock()
	defer d.mutex.Unlock()
	// TODO: use tx pool

	if req == nil || len(req.Reference) == 0 || len(req.Key) == 0 {
		return nil, store.ErrIllegalArguments
	}

	err := d.WaitForIndexingUpto(req.SinceTx)
	if err != nil {
		return nil, err
	}

	key := wrapWithPrefix(req.Key, setKeyPrefix)

	// check referenced key exists
	_, err = d.getAt(key, req.AtTx, 0, d.st, d.tx1)
	if err != nil {
		return nil, err
	}

	refKey := wrapWithPrefix(req.Reference, setKeyPrefix)
	refVal := wrapReferenceValueAt(key, req.AtTx)

	meta, err := d.st.Commit([]*store.KV{{Key: refKey, Value: refVal}})
	if err != nil {
		return nil, err
	}

	return schema.TxMetatadaTo(meta), err
}

//SafeReference ...
func (d *db) VerifiableSetReference(req *schema.VerifiableReferenceRequest) (*schema.VerifiableTx, error) {
	return nil, fmt.Errorf("Functionality not yet supported: %s", "VerifiableSetReference")
}

func wrapReferenceValueAt(key []byte, atTx uint64) []byte {
	refVal := make([]byte, 1+8+len(key))

	refVal[0] = referenceValuePrefix
	binary.BigEndian.PutUint64(refVal[1:], atTx)
	copy(refVal[1+8:], key)

	return refVal
}

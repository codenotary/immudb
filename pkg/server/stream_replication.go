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

package server

import "github.com/codenotary/immudb/pkg/api/schema"

func (s *ImmuServer) StreamTxs(req *schema.TxRequest, txsServer schema.ImmuService_StreamTxsServer) error {
	if req == nil || req.Tx == 0 {
		return ErrIllegalArguments
	}

	ind, err := s.getDbIndexFromCtx(txsServer.Context(), "StreamTxs")
	if err != nil {
		return err
	}

	db := s.dbList.GetByIndex(ind)

	currentTx := req.Tx

	for {
		err = db.WaitForIndexingUpto(currentTx, nil)
		if err != nil {
			return err
		}

		tx, err := db.TxByID(&schema.TxRequest{Tx: currentTx})
		if err != nil {
			return err
		}

		err = txsServer.Send(tx)
		if err != nil {
			return err
		}

		currentTx++
	}
}

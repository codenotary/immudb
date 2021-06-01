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

import (
	"bytes"

	"github.com/codenotary/immudb/pkg/api/schema"
)

func (s *ImmuServer) ExportTx(req *schema.TxRequest, txsServer schema.ImmuService_ExportTxServer) error {
	if req == nil || req.Tx == 0 {
		return ErrIllegalArguments
	}

	ind, err := s.getDbIndexFromCtx(txsServer.Context(), "ExportTx")
	if err != nil {
		return err
	}

	db := s.dbList.GetByIndex(ind)

	err = db.WaitForTx(req.Tx, nil)
	if err != nil {
		return err
	}

	bs, err := db.ExportTxByID(&schema.TxRequest{Tx: req.Tx})
	if err != nil {
		return err
	}

	sender := s.StreamServiceFactory.NewMsgSender(txsServer)

	err = sender.Send(bytes.NewReader(bs), len(bs))
	if err != nil {
		return err
	}

	return nil
}

func (s *ImmuServer) ReplicateTx(replicateTxServer schema.ImmuService_ReplicateTxServer) error {
	ind, err := s.getDbIndexFromCtx(replicateTxServer.Context(), "ReplicateTx")
	if err != nil {
		return err
	}

	receiver := s.StreamServiceFactory.NewMsgReceiver(replicateTxServer)

	bs, err := receiver.ReadFully()
	if err != nil {
		return err
	}

	db := s.dbList.GetByIndex(ind)

	md, err := db.ReplicateTx(bs)
	if err != nil {
		return err
	}

	return replicateTxServer.SendAndClose(md)
}

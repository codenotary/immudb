/*
Copyright 2022 Codenotary Inc. All rights reserved.

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
	"encoding/binary"

	"github.com/codenotary/immudb/pkg/api/schema"
	"google.golang.org/grpc/metadata"
)

func (s *ImmuServer) ExportTx(req *schema.ExportTxRequest, txsServer schema.ImmuService_ExportTxServer) error {
	if req == nil || req.Tx == 0 || txsServer == nil {
		return ErrIllegalArguments
	}

	db, err := s.getDBFromCtx(txsServer.Context(), "ExportTx")
	if err != nil {
		return err
	}

	bs, mayCommitUpToTxID, mayCommitUpToAlh, err := db.ExportTxByID(req)

	defer func() {
		if mayCommitUpToTxID > 0 {
			var bTxID [8]byte
			binary.BigEndian.PutUint64(bTxID[:], mayCommitUpToTxID)

			md := metadata.Pairs(
				"may-commit-up-to-txid-bin", string(bTxID[:]),
				"may-commit-up-to-alh-bin", string(mayCommitUpToAlh[:]),
			)

			txsServer.SetTrailer(md)
		}
	}()

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
	if replicateTxServer == nil {
		return ErrIllegalArguments
	}

	db, err := s.getDBFromCtx(replicateTxServer.Context(), "ReplicateTx")
	if err != nil {
		return err
	}

	if s.replicationInProgressFor(db.GetName()) {
		return ErrReplicationInProgress
	}

	receiver := s.StreamServiceFactory.NewMsgReceiver(replicateTxServer)

	bs, err := receiver.ReadFully()
	if err != nil {
		return err
	}

	md, err := db.ReplicateTx(bs)
	if err != nil {
		return err
	}

	return replicateTxServer.SendAndClose(md)
}

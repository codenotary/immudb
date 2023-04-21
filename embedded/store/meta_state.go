/*
Copyright 2023 Codenotary Inc. All rights reserved.

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

package store

import (
	"errors"
	"fmt"

	"github.com/codenotary/immudb/embedded/watchers"
)

var ErrAlreadyRunning = errors.New("already running")
var ErrAlreadyStopped = errors.New("already stopped")

type metaState struct {
	truncatedUpToTxID uint64

	indexes map[int]*indexSpec

	wHub *watchers.WatchersHub
}

type indexSpec struct {
	initialTxID uint64
	finalTxID   uint64
	initialTs   int64
	finalTs     int64
}

type metaStateOptions struct {
}

func openMetaState(path string, opts metaStateOptions) (*metaState, error) {
	return &metaState{
		indexes: make(map[int]*indexSpec),
		wHub:    watchers.New(0, MaxIndexID),
	}, nil
}

func (m *metaState) rollbackUpTo(txID uint64) error {
	m.indexes = make(map[int]*indexSpec)

	err := m.wHub.Close()
	if err != nil {
		return err
	}

	m.wHub = watchers.New(0, MaxIndexID)

	return nil
}

func (m *metaState) calculatedUpToTxID() uint64 {
	doneUpToTxID, _, _ := m.wHub.Status()
	return doneUpToTxID
}

func (m *metaState) processTxHeader(hdr *TxHeader) error {
	if hdr == nil {
		return ErrIllegalArguments
	}

	if m.calculatedUpToTxID() >= hdr.ID {
		return fmt.Errorf("%w: transaction already processed", ErrIllegalArguments)
	}

	if hdr.Metadata == nil {
		m.wHub.DoneUpto(hdr.ID)
		return nil
	}

	truncatedUpToTxID, err := hdr.Metadata.GetTruncatedTxID()
	if err == nil {
		if m.truncatedUpToTxID > truncatedUpToTxID {
			return ErrCorruptedData
		}

		m.truncatedUpToTxID = truncatedUpToTxID
	} else if !errors.Is(err, ErrTruncationInfoNotPresentInMetadata) {
		return err
	}

	indexingChanges := hdr.Metadata.GetIndexingChanges()

	if len(indexingChanges) > 0 {
		for id, change := range indexingChanges {
			_, indexAlreadyExists := m.indexes[id]

			if change.IsIndexDeletion() {
				if !indexAlreadyExists {
					return fmt.Errorf("%w: index does not exist", ErrCorruptedData)
				}

				delete(m.indexes, id)

				continue
			}

			if change.IsIndexCreation() {
				if indexAlreadyExists {
					return fmt.Errorf("%w: index already exists", ErrCorruptedData)
				}

				c := change.(*IndexCreationChange)

				m.indexes[id] = &indexSpec{
					initialTxID: c.InitialTxID,
					finalTxID:   c.FinalTxID,
					initialTs:   c.InitialTs,
					finalTs:     c.FinalTs,
				}

				continue
			}

			return fmt.Errorf("%w: it may be due to an unsupported metadata change", ErrUnexpectedError)
		}
	}

	m.wHub.DoneUpto(hdr.ID)

	return nil
}

func (m *metaState) close() error {
	return m.wHub.Close()
}

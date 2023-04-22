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
	"time"

	"github.com/codenotary/immudb/embedded/watchers"
)

var ErrAlreadyRunning = errors.New("already running")
var ErrAlreadyStopped = errors.New("already stopped")

type metaState struct {
	st *ImmuStore

	truncatedUpToTxID uint64

	indexes map[IndexID]*indexSpec

	wHub *watchers.WatchersHub

	stopCh chan (struct{})
	doneCh chan (struct{})

	running bool
}

type indexSpec struct {
	initialTxID uint64
	finalTxID   uint64
	initialTs   int64
	finalTs     int64
}

func newMetaState(st *ImmuStore) (*metaState, error) {
	//metaPath := filepath.Join(st.path, metaDirname)
	return &metaState{
		st:     st,
		wHub:   watchers.New(0, MaxIndexID),
		stopCh: make(chan struct{}),
		doneCh: make(chan struct{}),
	}, nil
}

func (m *metaState) calculatedUpToTxID() uint64 {
	doneUpToTxID, _, _ := m.wHub.Status()
	return doneUpToTxID
}

func (m *metaState) start() error {
	if m.running {
		return ErrAlreadyRunning
	}

	m.running = true

	go func() {
		for {
			select {
			case <-m.stopCh:
				{
					m.doneCh <- struct{}{}
					return
				}
			default:
				{
				}
			}

			txHdr, err := m.st.readTxHeader(m.calculatedUpToTxID()+1, false, false)
			if err != nil {
				if errors.Is(err, ErrTxNotFound) {
					time.Sleep(m.st.syncFrequency)
					continue
				}

				time.Sleep(10 * m.st.syncFrequency)
				m.st.notify(Error, false, "%s: while scanning transaction headers", err)
				continue
			}

			if txHdr.Metadata != nil {
				truncatedUpToTxID, err := txHdr.Metadata.GetTruncatedTxID()
				if err == nil {
					m.truncatedUpToTxID = truncatedUpToTxID
				} else if !errors.Is(err, ErrTxNotPresentInMetadata) {
					m.st.notify(Error, false, "%s: while reading transaction metadata", err)
					continue
				}

				indexingChanges := txHdr.Metadata.GetIndexingChanges()
				if len(indexingChanges) > 0 {

					for id, change := range indexingChanges {
						if change.IsIndexDeletion() {
							delete(m.indexes, id)
						}
						if change.IsIndexCreation() {
							c := change.(*IndexCreationChange)

							m.indexes[id] = &indexSpec{
								initialTxID: c.InitialTxID,
								finalTxID:   c.FinalTxID,
								initialTs:   c.InitialTs,
								finalTs:     c.FinalTs,
							}
						}
					}
				}
			}

			m.wHub.DoneUpto(txHdr.ID)
		}
	}()

	return nil
}

func (m *metaState) stop() error {
	if !m.running {
		return ErrAlreadyStopped
	}

	m.running = false

	m.stopCh <- struct{}{}
	<-m.doneCh

	return m.wHub.Close()
}

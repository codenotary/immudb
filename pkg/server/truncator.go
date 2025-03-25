/*
Copyright 2025 Codenotary Inc. All rights reserved.

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

package server

import (
	"time"

	"github.com/codenotary/immudb/pkg/database"
	"github.com/codenotary/immudb/pkg/truncator"
)

func (s *ImmuServer) isTruncatorRunningFor(db string) bool {
	_, ok := s.truncators[db]
	return ok
}

func (s *ImmuServer) startTruncatorFor(db database.DB, dbOpts *dbOptions) error {
	if !dbOpts.isDataRetentionEnabled() {
		s.Logger.Infof("Truncation for database '%s' is not required.", db.GetName())
		return ErrTruncatorNotNeeded
	}

	s.truncatorMutex.Lock()
	defer s.truncatorMutex.Unlock()

	if s.isTruncatorRunningFor(db.GetName()) {
		return database.ErrTruncatorAlreadyRunning
	}

	rp := time.Millisecond * time.Duration(dbOpts.RetentionPeriod)
	tf := time.Millisecond * time.Duration(dbOpts.TruncationFrequency)

	t := truncator.NewTruncator(db, rp, tf, s.Logger)
	err := t.Start()
	if err != nil {
		return err
	}

	s.truncators[db.GetName()] = t

	return nil
}

func (s *ImmuServer) stopTruncatorFor(db string) error {
	s.truncatorMutex.Lock()
	defer s.truncatorMutex.Unlock()

	t, ok := s.truncators[db]
	if !ok {
		return ErrTruncatorNotInProgress
	}

	err := t.Stop()
	if err == truncator.ErrTruncatorAlreadyStopped {
		return nil
	}
	if err != nil {
		return err
	}

	delete(s.truncators, db)

	return nil
}

func (s *ImmuServer) stopTruncation() {
	s.truncatorMutex.Lock()
	defer s.truncatorMutex.Unlock()

	for db, f := range s.truncators {
		err := f.Stop()
		if err != nil {
			s.Logger.Warningf("Error stopping truncator for '%s'. Reason: %v", db, err)
		} else {
			delete(s.truncators, db)
		}
	}
}

func (s *ImmuServer) getTruncatorFor(db string) (*truncator.Truncator, error) {
	s.truncatorMutex.Lock()
	defer s.truncatorMutex.Unlock()

	t, ok := s.truncators[db]
	if !ok {
		return nil, ErrTruncatorDoesNotExist
	}

	return t, nil
}

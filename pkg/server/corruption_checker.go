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

package server

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	mrand "math/rand"
	"sync"
	"time"

	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/codenotary/immudb/pkg/auth"
	"github.com/codenotary/immudb/pkg/logger"
	"github.com/codenotary/immudb/pkg/store"
)

// ErrConsistencyFail happens when a consistency check fails. Check the log to retrieve details on which element is failing
const ErrConsistencyFail = "consistency check fail at index %d"

type CCOptions struct {
	singleiteration    bool
	iterationSleepTime time.Duration
	frequencySleepTime time.Duration
}

type corruptionChecker struct {
	options        CCOptions
	dbList         DatabaseList
	Logger         logger.Logger
	Exit           bool
	Trusted        bool
	Wg             sync.WaitGroup
	currentDbIndex int
}

// CorruptionChecker corruption checker interface
type CorruptionChecker interface {
	Start(context.Context) (err error)
	Stop(context.Context)
	GetStatus(context.Context) bool
	Wait()
}

// NewCorruptionChecker returns new trust checker service
func NewCorruptionChecker(opt CCOptions, d DatabaseList, l logger.Logger) CorruptionChecker {
	return &corruptionChecker{
		options:        opt,
		dbList:         d,
		Logger:         l,
		Exit:           false,
		Trusted:        true,
		currentDbIndex: 0,
	}
}

// Start start the trust checker loop
func (s *corruptionChecker) Start(ctx context.Context) (err error) {
	s.Logger.Debugf("Start scanning ...")
	return s.checkLevel0(ctx)
}

// Stop stop the trust checker loop
func (s *corruptionChecker) Stop(ctx context.Context) {
	s.Exit = true
	s.Logger.Infof("Waiting for consistency checker to shut down")
	s.Wait()
}

// GetStatus return status of the trust checker. False means that a consistency checks was failed
func (s *corruptionChecker) GetStatus(ctx context.Context) bool {
	return s.Trusted
}

func (s *corruptionChecker) checkLevel0(ctx context.Context) (err error) {
	s.Wg.Add(1)
	s.Logger.Debugf("Retrieving a fresh root ...")
	if s.currentDbIndex == s.dbList.Length() {
		s.currentDbIndex = 0
	}
	db := s.dbList.GetByIndex(int64(s.currentDbIndex))
	s.currentDbIndex++
	var r *schema.Root
	if r, err = db.Store.CurrentRoot(); err != nil {
		s.Logger.Errorf("Error retrieving root: %s", err)
		return
	}
	if r.Root == nil {
		s.Logger.Debugf("Immudb is empty ...")
	} else {
		// create a range with all index presents in immudb
		ids := makeRange(0, r.Index)
		rn := mrand.New(newCryptoRandSource())
		// shuffle indexes
		rn.Shuffle(len(ids), func(i, j int) { ids[i], ids[j] = ids[j], ids[i] })
		s.Logger.Debugf("Start scanning %d elements", len(ids))
		for _, id := range ids {
			if s.Exit {
				s.Wg.Done()
				return
			}
			var item *schema.SafeItem
			if item, err = db.Store.BySafeIndex(schema.SafeIndexOptions{
				Index: id,
				RootIndex: &schema.Index{
					Index: r.Index,
				},
			}); err != nil {
				if err == store.ErrInconsistentDigest {
					auth.IsTampered = true
					s.Logger.Errorf("insertion order index %d was tampered", id)
					s.Wg.Done()
					return
				}
				s.Logger.Errorf("Error retrieving element at index %d: %s", id, err)
			}
			verified := item.Proof.Verify(item.Proof.Leaf, *r)
			s.Logger.Debugf("Item index %d, value %s, verified %t", item.Item.Index, item.Item.Value, verified)
			if !verified {
				s.Trusted = false
				auth.IsTampered = true
				s.Logger.Errorf(ErrConsistencyFail, item.Item.Index)
				s.Wg.Done()
				return
			}
			time.Sleep(s.options.frequencySleepTime)
		}
	}
	s.Wg.Done()
	s.sleep()
	if !s.Exit && !s.options.singleiteration {
		if err = s.checkLevel0(ctx); err != nil {
			return err
		}
	}
	return nil
}

func (s *corruptionChecker) sleep() {
	if !s.Exit {
		s.Logger.Debugf("Sleeping for some seconds ...")
		time.Sleep(s.options.iterationSleepTime)
	}
}

func makeRange(min, max uint64) []uint64 {
	a := make([]uint64, max-min+1)
	var i uint64
	for i = min; i <= max; i++ {
		a[i] = i
	}
	return a
}
func (s *corruptionChecker) Wait() {
	s.Wg.Wait()
}

type cryptoRandSource struct{}

func newCryptoRandSource() cryptoRandSource {
	return cryptoRandSource{}
}

func (cryptoRandSource) Int63() int64 {
	var b [8]byte
	_, _ = rand.Read(b[:])
	return int64(binary.LittleEndian.Uint64(b[:]) & (1<<63 - 1))
}

func (cryptoRandSource) Seed(_ int64) {}

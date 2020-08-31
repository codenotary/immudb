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
	exit           bool
	muxit          sync.Mutex
	Trusted        bool
	mux            sync.Mutex
	currentDbIndex int
	rg             RandomGenerator
}

// CorruptionChecker corruption checker interface
type CorruptionChecker interface {
	Start(context.Context) (err error)
	Stop()
	GetStatus() bool
}

// NewCorruptionChecker returns new trust checker service
func NewCorruptionChecker(opt CCOptions, d DatabaseList, l logger.Logger, rg RandomGenerator) CorruptionChecker {
	return &corruptionChecker{
		options:        opt,
		dbList:         d,
		Logger:         l,
		exit:           false,
		Trusted:        true,
		currentDbIndex: 0,
		rg:             rg,
	}
}

// Start start the trust checker loop
func (s *corruptionChecker) Start(ctx context.Context) (err error) {
	s.Logger.Debugf("Start scanning ...")

	for {
		s.mux.Lock()
		err = s.checkLevel0(ctx)
		s.mux.Unlock()

		if err != nil || s.isTerminated() || s.options.singleiteration {
			return err
		}

		time.Sleep(s.options.iterationSleepTime)
	}
}

func (s *corruptionChecker) isTerminated() bool {
	s.muxit.Lock()
	defer s.muxit.Unlock()
	return s.exit
}

// Stop stop the trust checker loop
func (s *corruptionChecker) Stop() {
	s.muxit.Lock()
	s.exit = true
	s.muxit.Unlock()
	s.Logger.Infof("Waiting for consistency checker to shut down")
	s.mux.Lock()
}

func (s *corruptionChecker) checkLevel0(ctx context.Context) (err error) {
	if s.currentDbIndex == s.dbList.Length() {
		s.currentDbIndex = 0
	}
	db := s.dbList.GetByIndex(int64(s.currentDbIndex))
	s.currentDbIndex++
	var r *schema.Root
	s.Logger.Debugf("Retrieving a fresh root ...")
	if r, err = db.Store.CurrentRoot(); err != nil {
		s.Logger.Errorf("Error retrieving root: %s", err)
		return
	}
	if r.Payload.Root == nil {
		s.Logger.Debugf("Immudb is empty ...")
	} else {
		// create a shuffle range with all indexes presents in immudb
		ids := s.rg.getList(0, r.Payload.Index)
		s.Logger.Debugf("Start scanning %d elements", len(ids))
		for _, id := range ids {
			if s.isTerminated() {
				return
			}
			var item *schema.SafeItem
			if item, err = db.Store.BySafeIndex(schema.SafeIndexOptions{
				Index: id,
				RootIndex: &schema.Index{
					Index: r.Payload.Index,
				},
			}); err != nil {
				if err == store.ErrInconsistentDigest {
					auth.IsTampered = true
					s.Logger.Errorf("insertion order index %d was tampered", id)
					return
				}
				s.Logger.Errorf("Error retrieving element at index %d: %s", id, err)
				return
			}
			verified := item.Proof.Verify(item.Proof.Leaf, *r)
			s.Logger.Debugf("Item index %d, value %s, verified %t", item.Item.Index, item.Item.Value, verified)
			if !verified {
				s.Trusted = false
				auth.IsTampered = true
				s.Logger.Errorf(ErrConsistencyFail, item.Item.Index)
				return
			}
			time.Sleep(s.options.frequencySleepTime)
		}
	}

	return nil
}

// GetStatus return status of the trust checker. False means that a consistency checks was failed
func (s *corruptionChecker) GetStatus() bool {
	return s.Trusted
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

type randomGenerator struct{}

type RandomGenerator interface {
	getList(uint64, uint64) []uint64
}

func (rg randomGenerator) getList(start, end uint64) []uint64 {
	ids := make([]uint64, end-start+1)
	var i uint64
	for i = start; i <= end; i++ {
		ids[i] = i
	}
	rn := mrand.New(newCryptoRandSource())
	// shuffle indexes
	rn.Shuffle(len(ids), func(i, j int) { ids[i], ids[j] = ids[j], ids[i] })
	return ids
}

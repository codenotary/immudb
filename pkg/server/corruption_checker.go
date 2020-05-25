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
	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/codenotary/immudb/pkg/logger"
	"github.com/codenotary/immudb/pkg/store"
	mrand "math/rand"
	"time"
)

// ErrConsistencyFail happens when a consistency check fails. Check the log to retrieve details on which element is failing
const ErrConsistencyFail = "consistency check fail at index %d"

type corruptionChecker struct {
	store      *store.Store
	Logger     logger.Logger
	Exit       bool
	StopImmudb func() error
	Trusted    bool
}

// ImmuTc trust checker interface
type CorruptionChecker interface {
	Start(context.Context) (err error)
	Stop(context.Context)
	GetStatus(context.Context) bool
}

// NewCorruptionChecker returns new trust checker service
func NewCorruptionChecker(s *store.Store, l logger.Logger, stopImmudb func() error) CorruptionChecker {
	return &corruptionChecker{s, l, false, stopImmudb, true}
}

// Start start the trust checker loop
func (s *corruptionChecker) Start(ctx context.Context) (err error) {
	s.Logger.Debugf("Start scanning ...")
	return s.checkLevel0(ctx)
}

// Stop stop the trust checker loop
func (s *corruptionChecker) Stop(ctx context.Context) {
	s.Exit = true
	s.StopImmudb()
}

// GetStatus return status of the trust checker. False means that a consistency checks was failed
func (s *corruptionChecker) GetStatus(ctx context.Context) bool {
	return s.Trusted
}

func (s *corruptionChecker) checkLevel0(ctx context.Context) (err error) {
	for ok := true; ok; ok = !s.Exit {
		s.Logger.Debugf("Retrieving a fresh root ...")
		var r *schema.Root
		if r, err = s.store.CurrentRoot(); err != nil {
			s.Logger.Errorf("Error retrieving root: %s", err)
			s.sleep()
			continue
		}
		if r.Root == nil {
			s.Logger.Debugf("Immudb is empty ...")
			s.sleep()
			continue
		}
		// create a range with all index presents in immudb
		ids := makeRange(0, r.Index)
		rn := mrand.New(newCryptoRandSource())
		// shuffle indexes
		rn.Shuffle(len(ids), func(i, j int) { ids[i], ids[j] = ids[j], ids[i] })
		s.Logger.Debugf("Start scanning %d elements", len(ids))
		for _, id := range ids {
			var item *schema.SafeItem

			if item, err = s.store.BySafeIndex(schema.SafeIndexOptions{
				Index: id,
				RootIndex: &schema.Index{
					Index: r.Index,
				},
			}); err != nil {
				if err == store.ErrInconsistentDigest {
					s.Stop(ctx)
					s.Logger.Errorf("insertion order index %d was tampered", id)
					break
				}
				s.Logger.Errorf("Error retrieving element at index %d: %s", id, err)
				continue
			}
			verified := item.Proof.Verify(item.Proof.Leaf, *r)
			s.Logger.Debugf("Item index %d, value %s, verified %t", item.Item.Index, item.Item.Value, verified)
			if !verified {
				s.Trusted = false
				s.Logger.Errorf(ErrConsistencyFail, item.Item.Index)
				s.Stop(ctx)
			}
		}
		s.sleep()
	}
	return s.checkLevel0(ctx)
}

func (s *corruptionChecker) sleep() {
	s.Logger.Debugf("Sleeping for some seconds ...")
	time.Sleep(10 * time.Second)
}

func makeRange(min, max uint64) []uint64 {
	a := make([]uint64, max-min+1)
	var i uint64
	for i = min; i <= max; i++ {
		a[i] = i
	}
	return a
}

type cryptoRandSource struct{}

func newCryptoRandSource() cryptoRandSource {
	return cryptoRandSource{}
}

func (_ cryptoRandSource) Int63() int64 {
	var b [8]byte
	_, _ = rand.Read(b[:])
	return int64(binary.LittleEndian.Uint64(b[:]) & (1<<63 - 1))
}

func (_ cryptoRandSource) Seed(_ int64) {}

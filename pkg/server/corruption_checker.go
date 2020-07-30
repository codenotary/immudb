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
	"crypto/sha256"
	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/codenotary/immudb/pkg/logger"
	"github.com/codenotary/immudb/pkg/store"
	"github.com/codenotary/merkletree"
	"math/rand"
	"sync"
	"time"
)

// ErrConsistencyFail happens when a consistency check fails. Check the log to retrieve details on which element is failing
const ErrConsistencyFail = "consistency check fail at index %d"

type corruptionChecker struct {
	options    CCOptions
	store      *store.Store
	Logger     logger.Logger
	Exit       bool
	StopImmudb func() error
	Trusted    bool
	Wg         sync.WaitGroup
}

type CCOptions struct {
	iterationSleepTime time.Duration
	frequencySleepTime time.Duration
}

// CorruptionChecker corruption checker interface
type CorruptionChecker interface {
	Start(context.Context) (err error)
	Stop(context.Context)
	GetStatus(context.Context) bool
	Wait()
}

// NewCorruptionChecker returns new trust checker service
func NewCorruptionChecker(cco CCOptions, s *store.Store, l logger.Logger, stopImmudb func() error) CorruptionChecker {
	return &corruptionChecker{
		options:    cco,
		store:      s,
		Logger:     l,
		Exit:       false,
		StopImmudb: stopImmudb,
		Trusted:    true}
}

// Start start the trust checker loop
func (s *corruptionChecker) Start(ctx context.Context) (err error) {
	s.Logger.Debugf("Start scanning ...")
	for !s.Exit {
		if err = s.checkLevel0(ctx); err != nil {
			s.Wg.Done()
			return err
		}
		//runtime.GC()
		s.sleep()
	}
	return err
}

// Stop stop the trust checker loop
func (s *corruptionChecker) Stop(ctx context.Context) {
	s.Exit = true
	s.Logger.Infof("Please wait for consistency checker shut down")
	s.Wait()
}

// GetStatus return status of the trust checker. False means that a consistency checks was failed
func (s *corruptionChecker) GetStatus(ctx context.Context) bool {
	return s.Trusted
}

func (s *corruptionChecker) checkLevel0(ctx context.Context) (err error) {
	s.Wg.Add(1)
	s.store.RLock()
	defer s.store.RUnlock()
	s.Logger.Debugf("Retrieving a fresh root ...")
	var r *schema.Root
	if r.Root == nil {
		s.Logger.Debugf("Immudb is empty ...")
	} else {
		rand.Seed(time.Now().UnixNano())
		id := random(0, r.Index)
		if s.Exit {
			s.Wg.Done()
			return
		}

		var ip *schema.InclusionProof
		ip, err = s.store.InclusionProof(schema.Index{Index: id})
		if err != nil {
			s.Logger.Errorf("Error fetching proof at element %d: %s", id, err)
			return
		}

		var path merkletree.Path
		path.FromSlice(ip.Path)
		var lf [sha256.Size]byte
		copy(lf[:], ip.Leaf)

		var root32 [32]byte
		copy(root32[:], r.Root[:32])
		verified := path.VerifyInclusion(r.Index, id, root32, lf)

		s.Logger.Debugf("Item index %d, verified %t", id, verified)
		if !verified {
			s.Trusted = false
			s.Logger.Errorf(ErrConsistencyFail, id)
			s.Wg.Done()
			s.StopImmudb()
			return
		}
		s.Wg.Done()
	}
	return nil
}

func (s *corruptionChecker) sleep() {
	if !s.Exit {
		s.Logger.Debugf("Sleeping for some seconds ...")
		time.Sleep(s.options.frequencySleepTime)
	}
}

func (s *corruptionChecker) Wait() {
	s.Wg.Wait()
}

const maxInt64 uint64 = 1<<63 - 1

func random(min, max uint64) uint64 {
	return randomHelper(max-min) + min
}

func randomHelper(n uint64) uint64 {
	if n < maxInt64 {
		return uint64(rand.Int63n(int64(n + 1)))
	}
	x := rand.Uint64()
	for x > n {
		x = rand.Uint64()
	}
	return x
}

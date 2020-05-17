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

package tc

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/codenotary/immudb/pkg/client"
	"github.com/codenotary/immudb/pkg/logger"
	mrand "math/rand"
	"time"
)

const ErrConsistencyFail = "consistency check fail at index %d"

type immuTc struct {
	Client  client.ImmuClient
	Logger  logger.Logger
	Quit    bool
	Trusted bool
}

type ImmuTc interface {
	Start(context.Context) (err error)
	Stop(context.Context)
	GetStatus(context.Context) bool
}

func NewImmuTc(c client.ImmuClient, l logger.Logger) ImmuTc {
	return &immuTc{c, l, false, true}
}

func (s *immuTc) Start(ctx context.Context) (err error) {
	s.Logger.Infof("Start scanning ...")
	return s.checkLevel0(ctx)
}

func (s *immuTc) Stop(ctx context.Context) {
	s.Quit = true
}

func (s *immuTc) GetStatus(ctx context.Context) bool {
	return s.Trusted
}

func (s *immuTc) checkLevel0(ctx context.Context) (err error) {
	for ok := true; ok; ok = !s.Quit {
		s.Logger.Debugf("Retrieving a fresh root ...")
		var r *schema.Root
		if r, err = s.Client.CurrentRoot(ctx); err != nil {
			return err
		}
		// create a range with all index presents in immudb
		ids := makeRange(0, r.Index)
		rn := mrand.New(newCryptoRandSource())
		// shuffle indexes
		rn.Shuffle(len(ids), func(i, j int) { ids[i], ids[j] = ids[j], ids[i] })

		for _, id := range ids {
			var item *client.VerifiedItem
			if item, err = s.Client.ByRawSafeIndex(ctx, id); err != nil {
				return err
			}
			s.Logger.Debugf("Item index %d, value %s, verified %t", item.Index, item.Value, item.Verified)
			if !item.Verified {
				s.Trusted = false
				s.Logger.Errorf(ErrConsistencyFail, item.Index)
			}
		}
		s.Logger.Debugf("Sleeping for some seconds ...")
		time.Sleep(10 * time.Second)
	}
	return s.checkLevel0(ctx)
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

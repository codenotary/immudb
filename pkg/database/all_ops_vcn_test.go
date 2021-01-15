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

package database

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/codenotary/immudb/pkg/api/schema"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
	"math/rand"
	"strings"
	"testing"
	"time"
)

type results struct {
	txMeta   *schema.TxMetadata
	workerId int
}

func TestOps_VcnLcExecOpsConcurrentTest(t *testing.T) {
	db, closer := makeDb()
	defer closer()

	const numExecAll = 10000
	jobs := make(chan *schema.ExecAllRequest, numExecAll)
	res := make(chan *results, numExecAll)
	errors := make(chan error, numExecAll)

	for w := 1; w <= 50; w++ {
		go execAll(w, db, jobs, res, errors)
	}

	for j := 1; j <= numExecAll; j++ {
		jobs <- getRandomExecOps()
	}
	close(jobs)

	for a := 1; a <= numExecAll; a++ {
		r := <-res
		//if r.txMeta.Id % 10 == 0{
		s := fmt.Sprintf("worker %d res %d", r.workerId, r.txMeta.Id)
		println(s)
		//}
		ts, _ := verifiedGetExt(db, r.txMeta.Id)
		//if r.txMeta.Id % 10 == 0{
		s = fmt.Sprintf(" verifiedGetExt %s", ts.String())
		println(s)
		//}
	}

}

func execAll(w int, db DB, jobs <-chan *schema.ExecAllRequest, res chan<- *results, errors chan<- error) {
	for j := range jobs {
		metaTx, err := db.ExecAll(j)
		if err != nil {
			errors <- err
		}
		r := &results{
			txMeta:   metaTx,
			workerId: w,
		}
		res <- r
	}
}

func verifiedGetExt(db DB, txId uint64) (*timestamppb.Timestamp, error) {
	// items transaction order is guaranteed by execAll operation.
	// Item date is inserted after notarized item (item in this case). So it will be the second item in the specified transaction
	tx, err := db.TxByID(&schema.TxRequest{Tx: txId})
	if err != nil {
		return nil, err
	}
	ItemDateKeyPrefix := append([]byte{0}, []byte("_ITEM.INSERTION-DATE.")...)
	if len(tx.Entries) < 2 && !bytes.HasPrefix(tx.Entries[1].Key, ItemDateKeyPrefix) {
		return nil, errors.New("missing")
	}
	date, err := db.Get(&schema.KeyRequest{Key: bytes.TrimPrefix(tx.Entries[1].Key, []byte{0}), AtTx: txId})
	if err != nil {
		if status.Code(err) == codes.NotFound {
			return nil, errors.New("missing")
		}
		return nil, fmt.Errorf("Error at transaction id : %d", txId)
	}
	t := time.Time{}
	err = t.UnmarshalBinary(date.Value)
	if err != nil {
		return nil, err
	}
	tspb := &timestamppb.Timestamp{Seconds: t.Unix(), Nanos: int32(t.Nanosecond())}
	return tspb, nil
}

func getRandomExecOps() *schema.ExecAllRequest {
	r := rand.Intn(10)
	time.Sleep(time.Duration(r) * time.Microsecond)

	tn1 := time.Now()

	keyItemDate := make([]byte, 8)
	binary.BigEndian.PutUint64(keyItemDate, uint64(tn1.UnixNano()))
	keyItemDate = bytes.Join([][]byte{[]byte("_ITEM.INSERTION-DATE."), keyItemDate}, nil)

	tb, _ := tn1.MarshalBinary()

	rand.Seed(tn1.UnixNano())
	chars := []rune("ABCDEFGHIJKLMNOPQRSTUVWXYZÅÄÖ" +
		"abcdefghijklmnopqrstuvwxyzåäö" +
		"0123456789")
	length := 8
	var b strings.Builder
	for i := 0; i < length; i++ {
		b.WriteRune(chars[rand.Intn(len(chars))])
	}

	sha256Token := sha256.Sum256([]byte(b.String()))

	prefix := []byte(`vcn.SGHn32iPIu87WQbNf8sEiTIq6V0_LztEzQdb4VmZImw=.`)
	key := append(prefix, sha256Token[:]...)
	aOps := &schema.ExecAllRequest{
		Operations: []*schema.Op{
			{
				Operation: &schema.Op_Kv{
					Kv: &schema.KeyValue{
						Key:   key,
						Value: []byte(`{"kind":"file","name":".gitignore","hash":"87b7515a98f78ed4ce0c6c7bb272e9ceb73e93770ac0ac98f98e1d1a085f7ba7","size":371,"timestamp":"0001-01-01T00:00:00Z","contentType":"application/octet-stream","metadata":{},"signer":"SGHn32iPIu87WQbNf8sEiTIq6V0_LztEzQdb4VmZImw=","status":0}`),
					},
				},
			},
			{
				Operation: &schema.Op_Kv{
					Kv: &schema.KeyValue{
						Key:   keyItemDate,
						Value: tb,
					},
				},
			},
			{
				Operation: &schema.Op_ZAdd{
					ZAdd: &schema.ZAddRequest{
						Set:      sha256Token[:],
						Key:      key,
						Score:    float64(tn1.UnixNano()),
						BoundRef: true,
					},
				},
			},
		},
	}

	return aOps
}

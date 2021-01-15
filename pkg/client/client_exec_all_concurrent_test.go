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
package client

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/codenotary/immudb/pkg/server"
	"github.com/codenotary/immudb/pkg/server/servertest"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"log"
	"math/rand"
	"os"
	"strings"
	"testing"
	"time"
)

func TestImmuClient_ExecAllConcurrent(t *testing.T) {
	options := server.DefaultOptions().WithAuth(true)
	bs := servertest.NewBufconnServer(options)

	bs.Start()
	defer bs.Stop()

	defer os.RemoveAll(options.Dir)
	defer os.Remove(".state-")

	ts := NewTokenService().WithTokenFileName("testTokenFile").WithHds(DefaultHomedirServiceMock())
	client, err := NewImmuClient(DefaultOptions().WithDialOptions(&[]grpc.DialOption{grpc.WithContextDialer(bs.Dialer), grpc.WithInsecure()}).WithTokenService(ts))
	if err != nil {
		log.Fatal(err)
	}
	lr, err := client.Login(context.TODO(), []byte(`immudb`), []byte(`immudb`))
	if err != nil {
		log.Fatal(err)
	}
	md := metadata.Pairs("authorization", lr.Token)
	ctx := metadata.NewOutgoingContext(context.Background(), md)

	const numExecAll = 10000
	jobs := make(chan *schema.ExecAllRequest, numExecAll)
	res := make(chan *results, numExecAll)
	errors := make(chan error, numExecAll)

	for w := 1; w <= 50; w++ {
		go execAll(w, client, ctx, jobs, res, errors)
	}

	for j := 1; j <= numExecAll; j++ {
		jobs <- getRandomExecOps()
	}
	close(jobs)

	for a := 1; a <= numExecAll; a++ {
		r := <-res
		s := fmt.Sprintf("worker %d res %d", r.workerId, r.txMeta.Id)
		println(s)
	}

	client.Disconnect()
}

type results struct {
	txMeta   *schema.TxMetadata
	workerId int
}

func getRandomExecOps() *schema.ExecAllRequest {
	//r := rand.Intn(10)
	//time.Sleep(time.Duration(r) * time.Microsecond)

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

func execAll(w int, client ImmuClient, ctx context.Context, jobs <-chan *schema.ExecAllRequest, res chan<- *results, errors chan<- error) {
	for j := range jobs {
		metaTx, err := client.ExecAll(ctx, j)
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

func TestImmuClient_ExecAllConcurrent2(t *testing.T) {
	options := server.DefaultOptions().WithAuth(true)
	bs := servertest.NewBufconnServer(options)

	bs.Start()
	defer bs.Stop()

	defer os.RemoveAll(options.Dir)
	defer os.Remove(".state-")

	const numExecAll = 10000
	jobs := make(chan *schema.ExecAllRequest, numExecAll)
	res := make(chan *results, numExecAll)
	errors := make(chan error, numExecAll)

	for w := 1; w <= 50; w++ {
		go fullExecAll(w, bs.Dialer, jobs, res, errors)
	}

	for j := 1; j <= numExecAll; j++ {
		jobs <- getRandomExecOps()
	}
	close(jobs)

	for a := 1; a <= numExecAll; a++ {
		r := <-res
		s := fmt.Sprintf("worker %d res %d", r.workerId, r.txMeta.Id)
		println(s)
	}

}

func fullExecAll(w int, Dialer servertest.BuffDialer, jobs <-chan *schema.ExecAllRequest, res chan<- *results, errors chan<- error) {

	ts := NewTokenService().WithTokenFileName("testTokenFile").WithHds(DefaultHomedirServiceMock())
	client, err := NewImmuClient(DefaultOptions().WithDialOptions(&[]grpc.DialOption{grpc.WithContextDialer(Dialer), grpc.WithInsecure()}).WithTokenService(ts))
	if err != nil {
		log.Fatal(err)
	}
	lr, err := client.Login(context.TODO(), []byte(`immudb`), []byte(`immudb`))
	if err != nil {
		log.Fatal(err)
	}
	md := metadata.Pairs("authorization", lr.Token)
	ctx := metadata.NewOutgoingContext(context.Background(), md)

	for j := range jobs {
		metaTx, err := client.ExecAll(ctx, j)
		if err != nil {
			errors <- err
		}
		r := &results{
			txMeta:   metaTx,
			workerId: w,
		}
		res <- r
	}
	client.Disconnect()

}

func TestImmuClient_ExecAllConcurrentIntegration(t *testing.T) {

	const numExecAll = 10000
	jobs := make(chan *schema.ExecAllRequest, numExecAll)
	res := make(chan *results, numExecAll)
	errors := make(chan error, numExecAll)

	for w := 1; w <= 50; w++ {
		go realExecAll(w, jobs, res, errors)
	}

	for j := 1; j <= numExecAll; j++ {
		jobs <- getRandomExecOps()
	}
	close(jobs)

	go func() {
		for a := 1; a <= numExecAll; a++ {
			err := <-errors
			e := fmt.Sprintf("error %s", err.Error())
			println(e)
		}
	}()
	for a := 1; a <= numExecAll; a++ {
		r := <-res
		s := fmt.Sprintf("worker %d res %d", r.workerId, r.txMeta.Id)
		println(s)
	}
}

func realExecAll(w int, jobs <-chan *schema.ExecAllRequest, res chan<- *results, errors chan<- error) {
	ts := NewTokenService().WithTokenFileName("testTokenFile").WithHds(DefaultHomedirServiceMock())
	client, err := NewImmuClient(DefaultOptions().WithDialOptions(&[]grpc.DialOption{grpc.WithInsecure()}).WithTokenService(ts))
	if err != nil {
		log.Fatal(err)
	}
	lr, err := client.Login(context.TODO(), []byte(`immudb`), []byte(`immudb`))
	if err != nil {
		log.Fatal(err)
	}
	md := metadata.Pairs("authorization", lr.Token)
	ctx := metadata.NewOutgoingContext(context.Background(), md)

	ur, err := client.UseDatabase(ctx, &schema.Database{Databasename: "defaultdb"})
	if err != nil {
		log.Fatal(err)
	}
	md = metadata.Pairs("authorization", ur.Token)
	ctx = metadata.NewOutgoingContext(context.Background(), md)

	for j := range jobs {
		metaTx, err := client.ExecAll(ctx, j)
		if err != nil {
			errors <- err
			return
		}
		r := &results{
			txMeta:   metaTx,
			workerId: w,
		}
		res <- r
	}
	client.Disconnect()
}

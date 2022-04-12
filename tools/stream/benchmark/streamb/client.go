/*
Copyright 2022 CodeNotary, Inc. All rights reserved.

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

package streamb

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/codenotary/immudb/pkg/client"
	"github.com/codenotary/immudb/pkg/stream"
	"github.com/codenotary/immudb/pkg/stream/streamtest"
	"github.com/golang/protobuf/ptypes/empty"
	"github.com/prometheus/common/log"
	"google.golang.org/grpc/metadata"
	"math/big"
	rand2 "math/rand"
	"os"
	"time"
)

func Benchmark(iterations, workers, maxValueSize, maxkvLength int, cleanIndex bool, cleanIndexFreq int) error {

	jobs := make(chan int, iterations)
	results := make(chan int, iterations)

	for w := 1; w <= workers; w++ {
		go worker(w, jobs, results, maxValueSize)
	}
	ticker := time.NewTicker(time.Duration(cleanIndexFreq) * time.Second)
	done := make(chan bool)
	if cleanIndex {
		go func() {
			for {
				select {
				case <-done:
					return
				case t := <-ticker.C:
					log.Info("Clean Index at ", t)
					err := CleanIndex()
					if err != nil {
						log.Info("Clean Index fail. Reason: %s", err)
					}
				}
			}
		}()
	}

	for j := 1; j <= iterations; j++ {
		kvl := 0
		if maxkvLength > 1 {
			kvl = rand2.Intn(maxkvLength-1) + 1
		}
		jobs <- kvl
	}
	close(jobs)

	for a := 1; a <= iterations; a++ {
		<-results
	}

	ticker.Stop()

	return nil
}

func worker(id int, jobs <-chan int, results chan<- int, maxValueSize int) {
	for j := range jobs {
		err := StreamContent(maxValueSize, j)
		if err != nil {
			log.Infof("worker %d failed. Reason: %s", id, err.Error())
			results <- 1
		} else {
			log.Infof("worker %d completed stream of %d kvs", id, j)
			results <- 0
		}
	}
}

func StreamContent(maxValueSize, kvLength int) error {
	cli, err := client.NewImmuClient(client.DefaultOptions())
	if err != nil {
		return err
	}
	lr, err := cli.Login(context.TODO(), []byte(`immudb`), []byte(`immudb`))
	if err != nil {
		return err
	}
	md := metadata.Pairs("authorization", lr.Token)
	ctx := metadata.NewOutgoingContext(context.Background(), md)

	ur, err := cli.UseDatabase(ctx, &schema.Database{Databasename: "defaultdb"})
	if err != nil {
		return err
	}

	md = metadata.Pairs("authorization", ur.Token)
	ctx = metadata.NewOutgoingContext(context.Background(), md)

	kvs := make([]*stream.KeyValue, kvLength)
	kvKeys := make([][]byte, kvLength)
	kvSHA256 := make([][]byte, kvLength)

	for i := 0; i < kvLength; i++ {
		size, err := rand.Int(rand.Reader, big.NewInt(int64(maxValueSize)))
		if err != nil {
			return err
		}
		f, err := streamtest.GenerateDummyFile("myFile", int(size.Int64()))
		if err != nil {
			return err
		}
		defer f.Close()
		defer os.Remove(f.Name())

		fi, err := f.Stat()
		if err != nil {
			return err
		}

		key := []byte(f.Name())
		kv := &stream.KeyValue{
			Key: &stream.ValueSize{
				Content: bufio.NewReader(bytes.NewBuffer(key)),
				Size:    len(key),
			},
			Value: &stream.ValueSize{
				Content: bufio.NewReader(f),
				Size:    int(fi.Size()),
			},
		}

		kvs[i] = kv
		kvKeys[i] = key

		sf, err := os.Open(f.Name())
		if err != nil {
			return err
		}
		h, err := streamtest.GetSHA256(sf)
		if err != nil {
			return err
		}
		kvSHA256[i] = h
	}

	txMeta, err := cli.StreamSet(ctx, kvs)
	if err != nil {
		kvsj, _ := json.Marshal(kvs)
		return fmt.Errorf("StreamSet returns error: %s on kv %s", err, kvsj)
	}

	for i := 0; i < kvLength; i++ {
		entry, err := cli.StreamGet(ctx, &schema.KeyRequest{Key: kvKeys[i], SinceTx: txMeta.Id})
		if err != nil {
			return fmt.Errorf("StreamGet returns error: %s on key %s", err, kvKeys[i])
		}

		final, err := streamtest.GetSHA256(bytes.NewBuffer(entry.Value))

		if bytes.Compare(kvSHA256[i], final) != 0 {
			return errors.New("file content is different")
		}
	}
	cli.Disconnect()
	return nil
}

func CleanIndex() error {
	cli, err := client.NewImmuClient(client.DefaultOptions())
	if err != nil {
		return err
	}
	lr, err := cli.Login(context.TODO(), []byte(`immudb`), []byte(`immudb`))
	if err != nil {
		return err
	}
	md := metadata.Pairs("authorization", lr.Token)
	ctx := metadata.NewOutgoingContext(context.Background(), md)

	ur, err := cli.UseDatabase(ctx, &schema.Database{Databasename: "defaultdb"})
	if err != nil {
		return err
	}

	md = metadata.Pairs("authorization", ur.Token)
	ctx = metadata.NewOutgoingContext(context.Background(), md)

	return cli.CleanIndex(ctx, &empty.Empty{})
}

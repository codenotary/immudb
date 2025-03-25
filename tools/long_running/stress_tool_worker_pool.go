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

package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"sync"
	"time"

	"github.com/codenotary/immudb/pkg/api/schema"
	immudb "github.com/codenotary/immudb/pkg/client"
	"google.golang.org/protobuf/types/known/emptypb"
)

// run stress tool worker pool. Ex stress_tool_worker_pool --committers 8 --readers 4 --duration 10s
type cfg struct {
	IpAddr     string
	Port       int
	Username   string
	Password   string
	DBName     string
	committers int
	readers    int
	duration   time.Duration
}

var config cfg

func parseConfig() (c cfg) {
	flag.StringVar(&c.IpAddr, "addr", "", "IP address of immudb server")
	flag.IntVar(&c.Port, "port", 3322, "Port number of immudb server")
	flag.StringVar(&c.Username, "user", "immudb", "Username for authenticating to immudb")
	flag.StringVar(&c.Password, "pass", "immudb", "Password for authenticating to immudb")
	flag.StringVar(&c.DBName, "db", "defaultdb", "Name of the database to use")
	flag.IntVar(&c.committers, "committers", 5, "number of concurrent committers. Each committer will open a session for each insertion")
	flag.IntVar(&c.readers, "readers", 5, "number of concurrent readers. Each reader will use a single session for all read")
	flag.DurationVar(&c.duration, "duration", time.Second*10, "duration of the test. Ex : 10s, 1m, 1h")
	flag.Parse()
	return
}

func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)
	config = parseConfig()
	jobs := entriesGenerator()
	okJobs := make(chan *schema.KeyValue)
	done := make(chan bool)
	doner := make(chan bool)

	wwg := new(sync.WaitGroup)
	rwg := new(sync.WaitGroup)

	for w := 1; w <= config.committers; w++ {
		go worker(jobs, okJobs, done, wwg)
	}
	for w := 1; w <= config.readers; w++ {
		go reader(okJobs, doner, rwg)
	}

	donec := make(chan bool)
	go compactor(donec)

	ticker := time.NewTicker(config.duration)
outer:
	for {
		select {
		case <-ticker.C:
			for w := 1; w <= config.committers; w++ {
				done <- true
			}
			wwg.Wait()
			for w := 1; w <= config.readers; w++ {
				doner <- true
			}
			donec <- true
			break outer
		}
	}
	rwg.Wait()
	log.Printf("done\n\n")
}

func worker(jobs, okJobs chan *schema.KeyValue, done chan bool, wwg *sync.WaitGroup) {
	log.Printf("worker started\n")
	var keyVal *schema.KeyValue
	for {
		select {
		case keyVal = <-jobs:
			wwg.Add(1)
			opts := immudb.DefaultOptions().WithAddress(config.IpAddr).WithPort(config.Port)

			var client immudb.ImmuClient
			var err error

			client = immudb.NewClient().WithOptions(opts)

			err = client.OpenSession(context.Background(), []byte(config.Username), []byte(config.Password), config.DBName)
			if err != nil {
				log.Fatalln("Failed to connect. Reason:", err)
			}

			time.Sleep(time.Millisecond * time.Duration(rand.Intn(5000)))

			_, err = client.Set(context.Background(), keyVal.Key, keyVal.Value)
			if err != nil && err.Error() != "session not found" {
				log.Fatalln("Failed to insert. Reason:", err)
			}

			err = client.CloseSession(context.Background())
			if err != nil && err.Error() != "session not found" {
				log.Fatalln("Failed to close session. Reason:", err)
			}
			okJobs <- keyVal
			wwg.Done()
		case <-done:
			return
		}
	}
}

func reader(okJobs chan *schema.KeyValue, done chan bool, rwg *sync.WaitGroup) {
	rwg.Add(1)
	log.Printf("reader started\n")

	var client immudb.ImmuClient
	var err error

	opts := immudb.DefaultOptions().WithAddress(config.IpAddr).WithPort(config.Port)
	client = immudb.NewClient().WithOptions(opts)

	err = client.OpenSession(context.Background(), []byte(config.Username), []byte(config.Password), config.DBName)
	if err != nil {
		log.Fatalln("Failed to connect. Reason:", err)
	}
	keyVal := &schema.KeyValue{}
outer:
	for {
		select {
		case keyVal = <-okJobs:
			_, err = client.VerifiedGet(context.Background(), keyVal.Key)
			if err != nil {
				log.Fatalln("Failed to get. Reason:", err)
			}
		case <-done:
			break outer
		}
	}
	err = client.CloseSession(context.Background())
	if err != nil && err.Error() != "session not found" {
		log.Fatalln("Failed to close session. Reason:", err)
	}
	rwg.Done()
}

func entriesGenerator() chan *schema.KeyValue {
	entries := make(chan *schema.KeyValue, 100)
	rand.Seed(time.Now().UnixNano())
	go func() {
		log.Printf("Worker is generating key values...\r\n")
		for true {
			id := int(time.Now().UnixNano())
			v := make([]byte, 32)
			rand.Read(v)
			entries <- &schema.KeyValue{Key: []byte(fmt.Sprintf("%d", id)), Value: v}
		}
	}()
	return entries
}

func compactor(done chan bool) {
	opts := immudb.DefaultOptions().WithAddress(config.IpAddr).WithPort(config.Port)
	client := immudb.NewClient().WithOptions(opts)

	err := client.OpenSession(context.Background(), []byte(config.Username), []byte(config.Password), config.DBName)
	if err != nil {
		log.Fatalln("Failed to connect. Reason:", err)
	}

	ticker := time.NewTicker(time.Second * 30)
outer:
	for {
		select {
		case <-ticker.C:
			log.Printf("Compaction started")
			err = client.CompactIndex(context.Background(), &emptypb.Empty{})
			if err != nil {
				log.Fatalln("Failed to compact. Reason:", err)
			}
			log.Printf("Compaction terminated")
		case <-done:
			break outer
		}
	}
	client.CloseSession(context.Background())
	return
}

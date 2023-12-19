/*
Copyright 2024 Codenotary Inc. All rights reserved.

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
	"log"
	"math/rand"
	"sync"
	"time"

	"github.com/codenotary/immudb/pkg/api/schema"
	immudb "github.com/codenotary/immudb/pkg/client"
	"google.golang.org/protobuf/types/known/emptypb"
)

type Entry struct {
	id    int
	value []byte
}

type cfg struct {
	IpAddr        string
	Port          int
	Username      string
	Password      string
	DBName        string
	committers    int
	kvCount       int
	vLen          int
	rndValues     bool
	readers       int
	rdCount       int
	readDelay     int
	readPause     int
	readRenew     bool
	compactDelay  int
	compactCycles int
	verifiers     int
	vrCount       int
	sessionMode   bool
	transactions  bool
}

func parseConfig() (c cfg) {
	flag.StringVar(&c.IpAddr, "addr", "", "IP address of immudb server")
	flag.IntVar(&c.Port, "port", 3322, "Port number of immudb server")
	flag.StringVar(&c.Username, "user", "immudb", "Username for authenticating to immudb")
	flag.StringVar(&c.Password, "pass", "immudb", "Password for authenticating to immudb")
	flag.StringVar(&c.DBName, "db", "defaultdb", "Name of the database to use")

	flag.IntVar(&c.committers, "committers", 10, "number of concurrent committers")
	flag.IntVar(&c.kvCount, "kvCount", 1_000, "number of kv entries per tx")
	flag.IntVar(&c.vLen, "vLen", 32, "value length (bytes)")
	flag.BoolVar(&c.rndValues, "rndValues", true, "values are randomly generated")

	flag.IntVar(&c.readers, "readers", 0, "number of concurrent readers")
	flag.IntVar(&c.rdCount, "rdCount", 100, "number of reads for each readers")
	flag.IntVar(&c.readDelay, "readDelay", 100, "Readers start delay (ms)")
	flag.IntVar(&c.readPause, "readPause", 0, "Readers pause at every cycle")
	flag.BoolVar(&c.readRenew, "readRenew", false, "renew snapshots on read")

	flag.IntVar(&c.compactDelay, "compactDelay", 0, "Milliseconds wait before compactions (0 disable)")
	flag.IntVar(&c.compactCycles, "compactCycles", 0, "Number of compaction to perform")

	flag.IntVar(&c.verifiers, "verifiers", 0, "number of verifiers readers")
	flag.IntVar(&c.vrCount, "vrCount", 100, "number of reads for each verifiers")

	flag.BoolVar(&c.sessionMode, "sessionMode", false, "use sessions auth mechanism mode")
	flag.BoolVar(&c.transactions, "transactions", false, "use transactions to insert data")

	flag.Parse()
	return
}

func connect(config cfg) (immudb.ImmuClient, context.Context) {
	opts := immudb.DefaultOptions().WithAddress(config.IpAddr).WithPort(config.Port)
	ctx := context.Background()

	var client immudb.ImmuClient
	var err error

	if config.sessionMode {
		client = immudb.NewClient()
		err = client.OpenSession(ctx, []byte(config.Username), []byte(config.Password), config.DBName)
		if err != nil {
			log.Fatalln("Failed to connect. Reason:", err)
		}
	} else {
		client, err = immudb.NewImmuClient(opts)
		if err != nil {
			log.Fatalln("Failed to connect. Reason:", err)
		}
		_, err = client.Login(ctx, []byte(config.Username), []byte(config.Password))
		if err != nil {
			log.Fatalln("Failed to login. Reason:", err.Error())
		}
		_, err = client.UseDatabase(ctx, &schema.Database{DatabaseName: config.DBName})
		if err != nil {
			log.Fatalln("Failed to use the database. Reason:", err)
		}
	}
	return client, ctx
}

func idGenerator(c cfg) chan int {
	// incremental id generator
	ids := make(chan int, 100)
	go func() {
		for true {
			ids <- int(time.Now().UnixNano())
		}
	}()
	return ids
}
func entriesGenerator(c cfg, ids chan int) chan Entry {
	entries := make(chan Entry, 100)
	rand.Seed(time.Now().UnixNano())
	go func() {
		log.Printf("Worker is generating rows...\r\n")
		for true {
			id := <-ids
			v := make([]byte, c.vLen)
			if c.rndValues {
				rand.Read(v)
			} else {
				copy(v, []byte("mariposa"))
			}
			entries <- Entry{id: id, value: v}
		}
	}()

	return entries
}

func committer(ctx context.Context, client immudb.ImmuClient, c cfg, entries chan Entry, cid int, wg *sync.WaitGroup) {
	log.Printf("Committer %d is inserting data...\r\n", cid)
	for i := 0; i < c.kvCount; i++ {
		entry := <-entries
		_, err := client.SQLExec(ctx, "INSERT INTO entries (id, value, ts) VALUES (@id, @value, now());",
			map[string]interface{}{"id": entry.id, "value": entry.value})
		if err != nil {
			log.Fatalf("Committer %d: Error while inserting value %d [%d]: %s", cid, entry.id, i, err)
		}
	}
	wg.Done()
	log.Printf("Committer %d done...\r\n", cid)
}

func committerWithTxs(ctx context.Context, client immudb.ImmuClient, c cfg, entries chan Entry, cid int, wg *sync.WaitGroup) {
	log.Printf("Transactions committer %d is inserting data...\r\n", cid)
	tx, err := client.NewTx(ctx)
	if err != nil {
		log.Fatalf("Transactions committer %d: Error while creating transaction: %s", cid, err)
	}
	for i := 0; i < c.kvCount; i++ {
		entry := <-entries
		err = tx.SQLExec(ctx, "INSERT INTO entries (id, value, ts) VALUES (@id, @value, now());",
			map[string]interface{}{"id": entry.id, "value": entry.value})
		if err != nil {
			log.Fatalf("Transactions committer %d: Error while inserting value %d [%d]: %s", cid, entry.id, i, err)
		}
	}
	_, err = tx.Commit(ctx)
	if err != nil {
		if err.Error() != "tx read conflict" {
			log.Fatalf("Transactions committer %d: Error while committing transaction: %s", cid, err)
		}
	}
	wg.Done()
	log.Printf("Transactions committer %d done...\r\n", cid)
}

func reader(ctx context.Context, client immudb.ImmuClient, c cfg, id int, wg *sync.WaitGroup) {
	if c.readDelay > 0 { // give time to populate db
		time.Sleep(time.Duration(c.readDelay) * time.Millisecond)
	}
	log.Printf("Reader %d is reading data\n", id)
	for i := 1; i <= c.rdCount; i++ {
		r, err := client.SQLQuery(ctx, "SELECT count() FROM entries where id<=@i;", map[string]interface{}{"i": i}, c.readRenew)
		if err != nil {
			log.Fatalf("Error querying val %d: %s", i, err.Error())
		}
		ret := r.Rows[0]
		n := ret.Values[0].GetN()
		if n != int64(i) {
			log.Printf("Reader %d read %d vs %d", id, n, i)
		}
		if c.readPause > 0 {
			time.Sleep(time.Duration(c.readPause) * time.Millisecond)
		}
	}
	wg.Done()
	log.Printf("Reader %d out\n", id)
}

func readerWithTxs(ctx context.Context, client immudb.ImmuClient, c cfg, id int, wg *sync.WaitGroup) {
	if c.readDelay > 0 { // give time to populate db
		time.Sleep(time.Duration(c.readDelay) * time.Millisecond)
	}
	log.Printf("Transactions reader %d is reading data\n", id)

	tx, err := client.NewTx(ctx)
	if err != nil {
		log.Fatalf("Transactions reader %d: Error while creating transaction: %s", id, err)
	}
	for i := 1; i <= c.rdCount; i++ {
		r, err := tx.SQLQuery(ctx, "SELECT count() FROM entries where id<=@i;", map[string]interface{}{"i": i})
		if err != nil {
			log.Fatalf("Error querying val %d: %s", i, err.Error())
		}
		ret := r.Rows[0]
		n := ret.Values[0].GetN()
		if n != int64(i) {
			log.Printf("Transactions reader %d read %d vs %d", id, n, i)
		}
		if c.readPause > 0 {
			time.Sleep(time.Duration(c.readPause) * time.Millisecond)
		}
	}
	_, err = tx.Commit(ctx)
	if err != nil {
		return
	}
	wg.Done()
	log.Printf("Transactions reader %d out\n", id)
}

func verifier(ctx context.Context, client immudb.ImmuClient, c cfg, id int, wg *sync.WaitGroup) {
	if c.readDelay > 0 { // give time to populate db
		time.Sleep(time.Duration(c.readDelay) * time.Millisecond)
	}
	log.Printf("Verifier %d is reading data\n", id)
	for i := 0; i < c.vrCount; i++ {
		idx := 1 + i*c.verifiers + id
		r, err := client.SQLQuery(ctx, "SELECT id, value, ts FROM entries WHERE id=@i;", map[string]interface{}{"i": idx}, c.readRenew)
		if err != nil {
			log.Fatalf("Error querying val %d: %s", i, err.Error())
		}
		if len(r.Rows) > 0 {
			row := r.Rows[0]
			err = client.VerifyRow(ctx, row, "entries", []*schema.SQLValue{row.Values[0]})
			if err != nil {
				log.Fatalf("Verification failed: verifier %d, id %d row %+v", id, idx, row)
			}
		} else {
			log.Printf("Verifier %d no results for id %d", id, idx)
		}
		if c.readPause > 0 {
			time.Sleep(time.Duration(c.readPause) * time.Millisecond)
		}
	}
	wg.Done()
	log.Printf("Verifier %d out\n", id)
}

func compactor(ctx context.Context, client immudb.ImmuClient, c cfg, wg *sync.WaitGroup) {
	for i := 0; i < c.compactCycles; i++ {
		time.Sleep(time.Duration(c.compactDelay) * time.Millisecond)
		log.Printf("Compaction %d started", i)
		client.CompactIndex(ctx, &emptypb.Empty{})
		log.Printf("Compaction %d terminated", i)
	}
	log.Printf("All compaction terminated")
	wg.Done()
}

func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)
	c := parseConfig()

	log.Println("Connecting...")
	client, ctx := connect(c)

	log.Printf("Creating tables\r\n")
	_, err := client.SQLExec(ctx, "CREATE TABLE IF NOT EXISTS entries (id INTEGER, value BLOB, ts INTEGER, PRIMARY KEY id);", nil)
	if err != nil {
		panic(err)
	}

	ids := idGenerator(c)
	entries := entriesGenerator(c, ids)

	wg := sync.WaitGroup{}

	for i := 0; i < c.committers; i++ {
		wg.Add(1)
		if c.sessionMode && c.transactions {
			go committerWithTxs(ctx, client, c, entries, i, &wg)
		} else {
			go committer(ctx, client, c, entries, i, &wg)
		}
	}

	for i := 0; i < c.readers; i++ {
		wg.Add(1)
		if c.sessionMode && c.transactions {
			go readerWithTxs(ctx, client, c, i, &wg)
		} else {
			go reader(ctx, client, c, i, &wg)
		}
	}
	for i := 0; i < c.verifiers; i++ {
		wg.Add(1)
		go verifier(ctx, client, c, i, &wg)
	}
	if c.compactDelay > 0 {
		wg.Add(1)
		go compactor(ctx, client, c, &wg)
	}
	wg.Wait()
	log.Printf("All operations done...\r\n")

	r, err := client.SQLQuery(ctx, "SELECT count() FROM  entries;", map[string]interface{}{}, true)
	if err != nil {
		panic(err)
	}
	row := r.Rows[0]
	count := row.Values[0].GetN()
	log.Printf("- Counted %d entries\n", count)
}

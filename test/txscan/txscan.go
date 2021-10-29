package main

import (
	"context"
	"flag"
	"log"
	"math/rand"
	"time"

	"github.com/codenotary/immudb/pkg/api/schema"
	immuclient "github.com/codenotary/immudb/pkg/client"
	"google.golang.org/grpc/metadata"
)

type dbitem struct {
	key  string
	val  string
	seen bool
}

var config struct {
	step int
	tot  int
}

func genvals(num int) ([]dbitem, map[string]int) {
	log.Printf("Generating %d values\n", config.tot)
	kvs := make([]dbitem, num)
	ptr := make(map[string]int, num)
	for i := 0; i < num; i++ {
		kvs[i].key = rndString(12)
		kvs[i].val = rndString(32)
		kvs[i].seen = false
		ptr[kvs[i].key] = i
	}
	return kvs, ptr
}

var seededRand *rand.Rand = rand.New(rand.NewSource(time.Now().UnixNano()))

const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

func rndString(l int) string {
	b := make([]byte, l)
	for i := range b {
		b[i] = charset[seededRand.Intn(len(charset))]
	}
	return string(b)
}

func connect() (immuclient.ImmuClient, context.Context) {
	client, err := immuclient.NewImmuClient(immuclient.DefaultOptions())
	if err != nil {
		log.Fatal(err)
	}
	ctx := context.Background()
	lr, err := client.Login(ctx, []byte(`immudb`), []byte(`immudb`))
	if err != nil {
		log.Fatal(err)
	}
	md := metadata.Pairs("authorization", lr.Token)
	ctx = metadata.NewOutgoingContext(context.Background(), md)
	return client, ctx
}

func init() {
	flag.IntVar(&config.step, "step", 50, "size of one batch")
	flag.IntVar(&config.tot, "tot", 1000, "total number of KV")
	flag.Parse()
}

func main() {
	kvs, ptr := genvals(config.tot)
	client, ctx := connect()
	insert(ctx, client, kvs)
	check(ctx, client, kvs, ptr)
}

func insert(ctx context.Context, client immuclient.ImmuClient, kvs []dbitem) {
	for i := 0; i < len(kvs); i += config.step {
		log.Printf("Inserting chunk %d-%d", i, i+config.step-1)
		rkvs := make([]*schema.KeyValue, config.step)
		for j := 0; j < config.step; j += 1 {
			rkvs[j] = &schema.KeyValue{Key: []byte(kvs[i+j].key), Value: []byte(kvs[i+j].val)}
		}
		req := schema.SetRequest{KVs: rkvs}
		_, err := client.SetAll(ctx, &req)
		if err != nil {
			log.Fatal("Error inserting: " + err.Error())
		}
	}
}

func check(ctx context.Context, client immuclient.ImmuClient, kvs []dbitem, ptr map[string]int) {
	for txn := 1; ; {
		req := schema.TxScanRequest{InitialTx: uint64(txn), Limit: 10, Desc: false}
		tx, err := client.TxScan(ctx, &req)
		if err != nil {
			log.Fatal(err)
		}
		if len(tx.Txs) == 0 {
			break
		}
		for i, t := range tx.Txs {
			log.Printf("IDX %d TX %d:\n", i, t.Header.Id)
			log.Printf("\tentries:\n")
			for _, e := range t.Entries {
				log.Printf("\t - %s\n", e.Key[1:])
				xkey := string(e.Key[1:])
				index, ok := ptr[xkey]
				if !ok {
					log.Fatal("Key out of nowhere:", xkey)
				}
				if kvs[index].key != xkey {
					log.Fatal("Key mismatch:", xkey, index)
				}
				kvs[index].seen = true
			}
		}
		txn = int(tx.Txs[len(tx.Txs)-1].Header.Id) + 1
	}
	for k, v := range kvs {
		if v.seen == false {
			log.Fatal("Key not read", v, k)
		}
	}
}

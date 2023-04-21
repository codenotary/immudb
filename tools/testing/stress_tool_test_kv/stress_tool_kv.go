package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"flag"
	"fmt"
	"log"
	"sync"
	"time"

	mrand "math/rand"

	"github.com/codenotary/immudb/pkg/api/schema"
	immudb "github.com/codenotary/immudb/pkg/client"
)

var (
	dbHostname = flag.String("host", "localhost", "immudb hostname")
	dbPort     = flag.Int("port", 3322, "immudb port")
	dbName     = flag.String("dbname", "defaultdb", "immudb database name")
	dbUser     = flag.String("user", "immudb", "immudb username")
	dbPassword = flag.String("password", "immudb", "immudb password")

	parallelism   = flag.Int("parallelism", 10, "number of parallel jobs")
	mixReadWrites = flag.Bool("mix-read-writes", false, "if true, mix read and write workloads")
	seed          = flag.Int("seed", 0, "test seed")
	totalEntries  = flag.Int("total-entries-written", 5_000_000, "total number of entries written during the test")
	totalReads    = flag.Int("total-entries-read", 10_000, "total number of entries read during the test")
	randKeyLen    = flag.Bool("randomize-key-length", false, "use randomized key lengths")

	help = flag.Bool("help", false, "show help")
)

type etaCalc struct {
	what       string
	start      time.Time
	lastReport time.Time
	totalCount int
}

func newEtaCalc(what string, totalCount int) *etaCalc {
	now := time.Now()
	return &etaCalc{
		what:       what,
		start:      now,
		lastReport: now,
		totalCount: totalCount,
	}
}

func (e *etaCalc) progress(progress int) {
	if time.Since(e.lastReport) >= time.Second {
		timeElapsed := time.Since(e.start)
		timeLeft := time.Duration(
			float64(timeElapsed) *
				(float64(e.totalCount-progress) / float64(progress)),
		)

		log.Printf("%s: Entries: %d, elapsed: %v, ETA: %v", e.what, progress, timeElapsed, timeLeft)
		e.lastReport = time.Now()
	}
}

func (e *etaCalc) printTotal() {
	log.Printf("%s: Finished in %v", e.what, time.Since(e.start))
}

func testRun(
	seed int,
	instance int,
	entriesCount int,
	readsCount int,
	performWrites bool,
	performReads bool,
) {

	ctx := context.Background()

	cl := immudb.NewClient().WithOptions(immudb.DefaultOptions().
		WithAddress(*dbHostname).
		WithPort(*dbPort),
	)
	err := cl.OpenSession(ctx, []byte(*dbUser), []byte(*dbPassword), *dbName)
	if err != nil {
		log.Fatal(err)
	}
	defer cl.CloseSession(ctx)

	var seedKey [sha256.Size]byte
	var seedVal [sha256.Size]byte

	s := sha256.Sum256([]byte(fmt.Sprintf("keyseed_%d_%d", seed, instance)))
	copy(seedKey[:], s[:])
	s = sha256.Sum256([]byte(fmt.Sprintf("valueseed_%d_%d", seed, instance)))
	copy(seedVal[:], s[:])

	key := func(i int) []byte {
		var b [8]byte
		copy(b[:], seedKey[:])
		binary.BigEndian.PutUint64(b[:], uint64(i))
		k := sha256.Sum256(b[:])
		kLen := len(k)
		if *randKeyLen {
			if k[kLen-1] > 200 {
				return bytes.Repeat(k[:], 1000/len(k))
			}

			kLen = 8

			minLen := 3
			kLen = int(k[len(k)-1])%(len(k)-minLen) + minLen
		}
		return k[:kLen]
	}

	val := func(i int) []byte {
		var b [8]byte
		copy(b[:], seedVal[:])
		binary.BigEndian.PutUint64(b[:], uint64(i))
		k := sha256.Sum256(b[:])
		return k[:]
	}

	if performWrites {

		tRep := newEtaCalc(fmt.Sprintf("Insertion %d", instance), entriesCount)
		batchSize := 1_000
		for i := 0; i < entriesCount; i += batchSize {

			alreadyAdded := map[string]struct{}{}
			kvs := []*schema.KeyValue{}
			for j := 0; j < batchSize && i+j < entriesCount; j++ {
				k := key(i + j)

				ks := string(k)
				if _, found := alreadyAdded[ks]; found {
					continue
				}
				alreadyAdded[ks] = struct{}{}

				kvs = append(kvs, &schema.KeyValue{
					Key:   key(i + j),
					Value: val(i + j),
				})
			}

			_, err := cl.SetAll(ctx, &schema.SetRequest{
				KVs: kvs,
			})
			if err != nil {
				log.Fatal(err)
			}

			tRep.progress(i)
		}
		tRep.printTotal()
	}

	if performReads {
		rnd := mrand.New(mrand.NewSource(int64(seed))) // We want predictable rand source
		tRep := newEtaCalc("Reading", readsCount)
		for i := 0; i < readsCount; i++ {

			j := rnd.Intn(entriesCount)

			v, err := cl.Get(ctx, key(j))
			if err != nil {
				log.Fatal("Error while reading back data ", err)
			}
			if !bytes.Equal(v.Value, val(j)) {
				log.Fatalf(
					"Invalid value read (%d)\n"+
						"key: %x\n"+
						"expected: %x\n"+
						"read: %x",
					j, key(j), val(j), v.Value,
				)
			}

			tRep.progress(i)
		}
		tRep.printTotal()
	}
}

func entriesForInstance(i int) int {
	// Instances could be a little bit skewed in the number of entries they process
	start := int(*totalEntries * i / *parallelism)
	end := int(*totalEntries * (i + 1) / *parallelism)

	return end - start
}

func readsForInstance(i int) int {
	// Instances could be a little bit skewed in the number of entries they process
	start := int(*totalReads * i / *parallelism)
	end := int(*totalReads * (i + 1) / *parallelism)

	return end - start
}

func main() {

	flag.Parse()

	if *help {
		flag.PrintDefaults()
		return
	}

	wg := sync.WaitGroup{}

	group1Size := *parallelism / 2

	// First half - run writes, reads will
	for i := 0; i < group1Size; i++ {
		wg.Add(1)
		go func(i int) {
			testRun(*seed, i, entriesForInstance(i), readsForInstance(i), true, !*mixReadWrites)
			wg.Done()
		}(i)
	}

	// For mixed workload, we have to wait for the writers to finish first, then we'll
	// overlap reads from the first group with writes in the second group
	if *mixReadWrites {
		wg.Wait()

		for i := 0; i < group1Size; i++ {
			wg.Add(1)
			go func(i int) {
				testRun(*seed, i, entriesForInstance(i), readsForInstance(i), false, true)
				wg.Done()
			}(i)
		}
	}

	// Second half of readers / writers
	for i := group1Size; i < *parallelism; i++ {
		wg.Add(1)
		go func(i int) {
			testRun(*seed, i, entriesForInstance(i), readsForInstance(i), true, true)
			wg.Done()
		}(i)
	}

	wg.Wait()
}

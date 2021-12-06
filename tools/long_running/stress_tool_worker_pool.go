package main

import (
	"context"
	"flag"
	"fmt"
	"github.com/codenotary/immudb/pkg/api/schema"
	immuclient "github.com/codenotary/immudb/pkg/client"
	"log"
	"math/rand"
	"sync"
	"time"
)

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
	okJobs := make(chan schema.KeyValue)
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
			break outer
		}
	}
	rwg.Wait()
	log.Printf("done\n\n")
}

func worker(jobs, okJobs chan schema.KeyValue, done chan bool, wwg *sync.WaitGroup) {
	log.Printf("worker started\n")
	var keyVal schema.KeyValue
	for {
		select {
		case keyVal = <-jobs:
			wwg.Add(1)
			opts := immuclient.DefaultOptions().WithAddress(config.IpAddr).WithPort(config.Port)

			var client immuclient.ImmuClient
			var err error

			client = immuclient.DefaultClient().WithOptions(opts)

			err = client.OpenSession(context.TODO(), []byte(config.Username), []byte(config.Password), config.DBName)
			if err != nil {
				log.Fatalln("Failed to connect. Reason:", err)
			}

			time.Sleep(time.Millisecond * time.Duration(rand.Intn(5000)))

			_, err = client.Set(context.TODO(), keyVal.Key, keyVal.Value)
			if err != nil && err.Error() != "session not found" {
				log.Fatalln("Failed to insert. Reason:", err)
			}

			err = client.CloseSession(context.TODO())
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

func reader(okJobs chan schema.KeyValue, done chan bool, rwg *sync.WaitGroup) {
	rwg.Add(1)
	log.Printf("reader started\n")

	var client immuclient.ImmuClient
	var err error

	opts := immuclient.DefaultOptions().WithAddress(config.IpAddr).WithPort(config.Port)
	client = immuclient.DefaultClient().WithOptions(opts)

	err = client.OpenSession(context.TODO(), []byte(config.Username), []byte(config.Password), config.DBName)
	if err != nil {
		log.Fatalln("Failed to connect. Reason:", err)
	}
	keyVal := schema.KeyValue{}
outer:
	for {
		select {
		case keyVal = <-okJobs:
			_, err = client.VerifiedGet(context.TODO(), keyVal.Key)
			if err != nil {
				log.Fatalln("Failed to get. Reason:", err)
			}
		case <-done:
			break outer
		}
	}
	err = client.CloseSession(context.TODO())
	if err != nil && err.Error() != "session not found" {
		log.Fatalln("Failed to close session. Reason:", err)
	}
	rwg.Done()
}

func entriesGenerator() chan schema.KeyValue {
	entries := make(chan schema.KeyValue, 100)
	rand.Seed(time.Now().UnixNano())
	go func() {
		log.Printf("Worker is generating key values...\r\n")
		for true {
			id := int(time.Now().UnixNano())
			v := make([]byte, 32)
			rand.Read(v)
			entries <- schema.KeyValue{Key: []byte(fmt.Sprintf("%d", id)), Value: v}
		}
	}()
	return entries
}

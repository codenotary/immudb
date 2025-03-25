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
	"time"

	"github.com/codenotary/immudb/pkg/api/schema"
	immuclient "github.com/codenotary/immudb/pkg/client"
)

var config struct {
	IpAddr    string
	Port      int
	Username  string
	Password  string
	DBName    string
	Retention time.Duration
	Interval  time.Duration
	Frequency time.Duration
}

func init() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)
	flag.StringVar(&config.IpAddr, "addr", "", "IP address of immudb server")
	flag.IntVar(&config.Port, "port", 3322, "Port number of immudb server")
	flag.StringVar(&config.Username, "user", "immudb", "Username for authenticating to immudb")
	flag.StringVar(&config.Password, "pass", "immudb", "Password for authenticating to immudb")
	flag.StringVar(&config.DBName, "db", "defaultdb", "Name of the database to use")
	flag.DurationVar(&config.Retention, "retention", 5*time.Minute, "Data retention period")
	flag.DurationVar(&config.Frequency, "frequency", 5*time.Minute, "Frequence between retention enforcement")
	flag.DurationVar(&config.Interval, "interval", 5*time.Second, "Time between data injection")
	flag.Parse()
	rand.Seed(time.Now().UnixNano())
}

func connect() (context.Context, immuclient.ImmuClient) {
	ctx := context.Background()
	opts := immuclient.DefaultOptions().WithAddress(config.IpAddr).WithPort(config.Port)
	client := immuclient.NewClient().WithOptions(opts)
	err := client.OpenSession(ctx, []byte(config.Username), []byte(config.Password), "defaultdb")
	dblist, err := client.DatabaseListV2(ctx)
	if err != nil {
		log.Fatalln("Failed to list. Reason:", err)
	}
	db_exists := false
	for _, db := range dblist.Databases {
		if db.Name == config.DBName {
			db_exists = true
			break
		}
	}
	if !db_exists {
		log.Printf("Creating database %s", config.DBName)
		settings := &schema.DatabaseNullableSettings{
			FileSize: &schema.NullableUint32{Value: 4096},
			TruncationSettings: &schema.TruncationNullableSettings{
				RetentionPeriod: &schema.NullableMilliseconds{Value: config.Retention.Milliseconds()},
			},
		}
		client.CreateDatabaseV2(ctx, config.DBName, settings)
	} else {
		log.Printf("Database %s already exists", config.DBName)
	}
	client.CloseSession(ctx)
	err = client.OpenSession(ctx, []byte(config.Username), []byte(config.Password), config.DBName)
	if err != nil {
		log.Fatalln("Failed to connect. Reason:", err)
	}
	return ctx, client
}

var chars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890-"

func randStr(length int) []byte {
	ll := len(chars)
	b := make([]byte, length)
	rand.Read(b) // generates len(b) random bytes
	for i := 0; i < length; i++ {
		b[i] = chars[int(b[i])%ll]
	}
	return b
}

func insertData(ic immuclient.ImmuClient, ctx context.Context) {
	k := []byte(fmt.Sprintf("key:%d", time.Now().Unix()))
	v := randStr(256)
	_, err := ic.Set(ctx, k, v)
	if err != nil {
		log.Fatal("Error inserting data: ", err)
	}
}

func flushData(ic immuclient.ImmuClient, ctx context.Context) {
	err := ic.TruncateDatabase(ctx, config.DBName, config.Retention)
	if err != nil {
		log.Printf("Error truncating database: %s", err.Error())
	}
}

func main() {
	log.Printf("Starting")
	ctx, client := connect()
	tickerI := time.NewTicker(config.Interval)
	tickerR := time.NewTicker(config.Frequency)
	for {
		select {
		case <-tickerI.C:
			log.Printf("Inserting row")
			insertData(client, ctx)
		case <-tickerR.C:
			log.Printf("Truncating")
			flushData(client, ctx)
		}
	}
}

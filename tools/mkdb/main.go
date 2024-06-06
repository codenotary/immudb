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

	"github.com/codenotary/immudb/pkg/api/schema"
	immuclient "github.com/codenotary/immudb/pkg/client"
)

var config struct {
	IpAddr   string
	Port     int
	Username string
	Password string

	DBName             string
	CacheSize          int
	FlushThreshold     int
	MaxActiveSnapshots int
	FileSize           int
	MaxKeyLen          int
	MaxValueLen        int
	MaxTxEntries       int
	MaxConcurrency     int
}

func init() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)
	flag.StringVar(&config.IpAddr, "addr", "", "IP address of immudb server")
	flag.IntVar(&config.Port, "port", 3322, "Port number of immudb server")
	flag.StringVar(&config.Username, "user", "immudb", "Username for authenticating to immudb")
	flag.StringVar(&config.Password, "pass", "immudb", "Password for authenticating to immudb")
	flag.StringVar(&config.DBName, "db", "defaultdb", "Name of the database to use")

	flag.IntVar(&config.CacheSize, "cachesize", 100_000, "Cache Size")
	flag.IntVar(&config.FlushThreshold, "flushthreshold", 100_000, "Cache Size")
	flag.IntVar(&config.MaxActiveSnapshots, "maxactivesnapshots", 100, "max active snapshots")

	flag.IntVar(&config.FileSize, "FileSize", 100, "Chunk file size")
	flag.IntVar(&config.MaxKeyLen, "MaxKeyLen", 100, "max length of keys")
	flag.IntVar(&config.MaxValueLen, "MaxValueLen", 100, "max length of values")
	flag.IntVar(&config.MaxTxEntries, "MaxTxEntries", 100, "max Entries per transaction")
	flag.IntVar(&config.MaxConcurrency, "MaxConcurrency", 100, "max concurrency")
	flag.Parse()
}

func main() {
	ctx := context.Background()
	opts := immuclient.DefaultOptions().WithAddress(config.IpAddr).WithPort(config.Port)
	client := immuclient.NewClient().WithOptions(opts)
	err := client.OpenSession(ctx, []byte(config.Username), []byte(config.Password), "defaultdb")
	if err != nil {
		log.Fatalln("Failed to open session. Reason:", err)
	}
	defer client.CloseSession(ctx)

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
	if db_exists {
		log.Printf("Database %s already exists", config.DBName)
	}
	log.Printf("Creating database %s", config.DBName)
	myindexOptions := schema.IndexNullableSettings{
		CacheSize:          &schema.NullableUint32{Value: uint32(config.CacheSize)},
		FlushThreshold:     &schema.NullableUint32{Value: uint32(config.FlushThreshold)},
		MaxActiveSnapshots: &schema.NullableUint32{Value: uint32(config.MaxActiveSnapshots)},
	}
	settings := &schema.DatabaseNullableSettings{
		IndexSettings:  &myindexOptions,
		FileSize:       &schema.NullableUint32{Value: uint32(config.FileSize)},
		MaxKeyLen:      &schema.NullableUint32{Value: uint32(config.MaxKeyLen)},
		MaxValueLen:    &schema.NullableUint32{Value: uint32(config.MaxValueLen)},
		MaxTxEntries:   &schema.NullableUint32{Value: uint32(config.MaxTxEntries)},
		MaxConcurrency: &schema.NullableUint32{Value: uint32(config.MaxConcurrency)},
	}
	_, err = client.CreateDatabaseV2(ctx, config.DBName, settings)
	if err != nil {
		log.Fatalln("Failed to create database. Reason:", err)
	}
	log.Printf("Database %s created", config.DBName)
}

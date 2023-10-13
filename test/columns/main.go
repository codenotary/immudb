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

package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"strings"
	"math/rand"

	immuclient "github.com/codenotary/immudb/pkg/client"
)

var config struct {
	IpAddr   string
	Port     int
	Username string
	Password string
	DBName   string
	NCycles  int
	MaxCol   int
}

func init() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)
	flag.StringVar(&config.IpAddr, "addr", "", "IP address of immudb server")
	flag.IntVar(&config.Port, "port", 3322, "Port number of immudb server")
	flag.StringVar(&config.Username, "user", "immudb", "Username for authenticating to immudb")
	flag.StringVar(&config.Password, "pass", "immudb", "Password for authenticating to immudb")
	flag.StringVar(&config.DBName, "db", "defaultdb", "Name of the database to use")
	flag.IntVar(&config.NCycles, "ncycles", 1000, "Number of add/remove cycles")
	flag.IntVar(&config.MaxCol, "maxcol", 100, "Number of columns present")

	flag.Parse()
}

func connect() (context.Context, immuclient.ImmuClient) {
	ctx := context.Background()
	opts := immuclient.DefaultOptions().WithAddress(config.IpAddr).WithPort(config.Port)
	client := immuclient.NewClient().WithOptions(opts)
	err := client.OpenSession(ctx, []byte(config.Username), []byte(config.Password), config.DBName)
	if err != nil {
		log.Fatalln("Failed to open session. Reason:", err)
	}
	return ctx, client
}

type worker struct {
	ctx     context.Context
	client  immuclient.ImmuClient
	tabname string
	cols    []string
}

func (w *worker) exec(statement string) {
	tx, err := w.client.NewTx(w.ctx)
	if err != nil {
		log.Fatalln("Failed to create transaction. Reason:", err)
	}
	err = tx.SQLExec(w.ctx, statement, nil)
	if err != nil {
		log.Printf("Error in statement: %s", err.Error())
	}
	_, err = tx.Commit(w.ctx)
	if err != nil {
		log.Fatalln("Failed to execute statement. Reason:", err)
	}
}

func (w *worker) mktable() {
	w.tabname = fmt.Sprintf("tab%s", getRnd())
	s := fmt.Sprintf("CREATE TABLE %s (id UUID, PRIMARY KEY id)", w.tabname)
	w.exec(s)
	log.Printf("Table %s created", w.tabname)
}

func (w *worker) initcols() {
	for i := 0; i < config.MaxCol; i++ {
		w.addcol()
	}
}

func (w *worker) addcol() {
	colname := fmt.Sprintf("C%s", getRnd())
	w.cols = append(w.cols, colname)
	s := fmt.Sprintf("alter table %s add column %s varchar[12]", w.tabname, colname)
	w.exec(s)
	log.Printf("Added column %s", colname)
}

func (w *worker) delcol() {
	idx := rand.Intn(len(w.cols))
	colname := w.cols[idx]
	s := fmt.Sprintf("alter table %s drop column %s", w.tabname, colname)
	w.exec(s)
	w.cols = append(w.cols[:idx],w.cols[idx+1:]...)
	log.Printf("Removed column %s", colname)
}

func (w *worker) addline() {
	l := len(w.cols)
	v := make([]string, l)
	for i := 0; i < l; i++ {
		v[i] = "'"+getRnd()+"'"
	}
	cols := strings.Join(w.cols, ",")
	vals := strings.Join(v, ",")
	q := fmt.Sprintf("INSERT INTO %s(id, %s) values (random_uuid(),%s)", w.tabname, cols, vals)
	log.Print(q)
	w.exec(q)
}

func main() {
	startRnd(8)
	ctx, client := connect()
	w := &worker{
		ctx:    ctx,
		client: client,
	}
	w.mktable()
	w.initcols()
	for i := 0; i < config.NCycles; i++ {
		w.addline()
		w.addcol()
		w.delcol()
	}
}

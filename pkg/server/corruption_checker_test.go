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

package server

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/codenotary/immudb/pkg/logger"
	"github.com/stretchr/testify/assert"
)

func TestEmptyDBCorruptionChecker(t *testing.T) {

	var err error
	dbList := NewDatabaseList()
	db, _ := makeDb()
	dbList.Append(db)

	cco := CCOptions{}
	cco.iterationSleepTime = 1 * time.Millisecond
	cco.frequencySleepTime = 1 * time.Millisecond
	cco.singleiteration = true

	cc := NewCorruptionChecker(cco, dbList, &mockLogger{})

	err = cc.Start(context.TODO())

	for i := 0; i < dbList.Length(); i++ {
		val := dbList.GetByIndex(int64(i))
		val.Store.Close()
	}
	assert.Nil(t, err)
}

func TestCorruptionChecker(t *testing.T) {
	var err error
	dbList := NewDatabaseList()
	db, _ := makeDb()
	kv := &schema.KeyValue{
		Key:   []byte(strconv.FormatUint(1, 10)),
		Value: []byte(strconv.FormatUint(2, 10)),
	}
	db.Set(kv)
	dbList.Append(db)

	cco := CCOptions{}
	cco.iterationSleepTime = 1 * time.Millisecond
	cco.frequencySleepTime = 1 * time.Millisecond
	cco.singleiteration = true

	cc := NewCorruptionChecker(cco, dbList, &mockLogger{})

	err = cc.Start(context.TODO())

	for i := 0; i < dbList.Length(); i++ {
		val := dbList.GetByIndex(int64(i))
		val.Store.Close()
	}
	assert.Nil(t, err)
}

type mockLogger struct{}

func (l *mockLogger) Errorf(f string, v ...interface{}) {}

func (l *mockLogger) Warningf(f string, v ...interface{}) {}

func (l *mockLogger) Infof(f string, v ...interface{}) {}

func (l *mockLogger) Debugf(f string, v ...interface{}) {}

<<<<<<< HEAD
func (l *mockLogger) CloneWithLevel(level logger.LogLevel) logger.Logger { return l }
=======
func (l *mockLogger) CloneWithLevel(level logger.LogLevel) logger.Logger {
	return &logger.FileLogger{}
}
>>>>>>> tests(pkg/server): mtlsoptions

/*
Copyright 2021 CodeNotary, Inc. All rights reserved.

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
	"github.com/codenotary/immudb/pkg/logger"
)

/*
import (
	"testing"

	"github.com/codenotary/immudb/pkg/logger"
)


func makeDb() (database.DB, func()) {
	dbName := "EdithPiaf" + strconv.FormatInt(time.Now().UnixNano(), 10)
	options := database.DefaultOption().WithDbName(dbName).WithInMemoryStore(true).WithCorruptionChecker(false)
	db, err := database.NewDb(options, logger.NewSimpleLogger("immudb ", os.Stderr))
	if err != nil {
		log.Fatalf("Error creating Db instance %s", err)
	}

	return db, func() {
		if err := db.Close(); err != nil {
			log.Fatal(err)
		}
		if err := os.RemoveAll(options.GetDbName()); err != nil {
			log.Fatal(err)
		}
	}
}

func TestEmptyDBCorruptionChecker(t *testing.T) {

	var err error
	dbList := NewDatabaseList()
	db, _ := makeDb()
	dbList.Append(db)

	cco := CCOptions{}
	cco.iterationSleepTime = 1 * time.Millisecond
	cco.frequencySleepTime = 1 * time.Millisecond
	cco.singleiteration = true

	cc := NewCorruptionChecker(cco, dbList, &mockLogger{}, randomGenerator{})

	err = cc.Start(context.TODO())

	for i := 0; i < dbList.Length(); i++ {
		val := dbList.GetByIndex(int64(i))
		val.Close()
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

	time.Sleep(500 * time.Millisecond)
	cco := CCOptions{}
	cco.iterationSleepTime = 1 * time.Millisecond
	cco.frequencySleepTime = 1 * time.Millisecond
	cco.singleiteration = true

	cc := NewCorruptionChecker(cco, dbList, &mockLogger{}, randomGenerator{})

	err = cc.Start(context.TODO())

	for i := 0; i < dbList.Length(); i++ {
		val := dbList.GetByIndex(int64(i))
		val.Close()
	}
	assert.Nil(t, err)
}

func TestCorruptionCheckerOnTamperInsertionOrderIndexDb(t *testing.T) {
	var err error
	defer os.RemoveAll("test")
	dbList := NewDatabaseList()
	options := database.DefaultOption().WithDbName("test").WithDbRootPath("test")
	db, err := database.NewDb(options, logger.NewSimpleLogger("immudb ", os.Stderr))
	if err != nil {
		log.Fatal(err)
	}
	k := []byte(strconv.FormatUint(1, 10))
	v := []byte(strconv.FormatUint(2, 10))
	kv := &schema.KeyValue{
		Key:   k,
		Value: v,
	}
	if _, err = db.Set(kv); err != nil {
		log.Fatal(err)
	}
	db.Close()
	// Tampering
	opts := badger.DefaultOptions("test/test").WithLogger(nil)
	dbb, err := badger.OpenManaged(opts)
	if err != nil {
		log.Fatal(err)
	}
	txn := dbb.NewTransactionAt(math.MaxUint64, true)
	defer txn.Discard()
	item, err := txn.Get(k)
	if err != nil {
		log.Fatal(err)
	}
	value, err := item.ValueCopy(nil)
	if err != nil {
		log.Fatal(err)
	}
	ts := binary.BigEndian.Uint64(value[:8])
	v1 := append(value[:8], []byte(strconv.FormatUint(3, 10))...)
	if err := txn.Set(k, v1); err != nil {
		log.Fatal(err)
	}
	if err := txn.CommitAt(ts, nil); err != nil {
		log.Fatal(err)
	}
	dbb.Close()
	// End Tampering
	db1, err := database.OpenDb(options, logger.NewSimpleLogger("immudb ", os.Stderr))
	assert.NoError(t, err)
	dbList.Append(db1)

	time.Sleep(500 * time.Millisecond)
	cco := CCOptions{}
	cco.iterationSleepTime = 1 * time.Millisecond
	cco.frequencySleepTime = 1 * time.Millisecond
	cco.singleiteration = true

	cc := NewCorruptionChecker(cco, dbList, &mockLogger{}, randomGenerator{})

	err = cc.Start(context.TODO())

	for i := 0; i < dbList.Length(); i++ {
		val := dbList.GetByIndex(int64(i))
		val.Close()
	}
	assert.Error(t, err)
}

func TestCorruptionCheckerOnTamperDbInconsistentState(t *testing.T) {
	var err error
	defer os.RemoveAll("test")
	dbList := NewDatabaseList()
	options := database.DefaultOption().WithDbName("test").WithDbRootPath("test")
	db, err := database.NewDb(options, logger.NewSimpleLogger("immudb ", os.Stderr))
	if err != nil {
		log.Fatal(err)
	}
	k := []byte(strconv.FormatUint(1, 10))
	v := []byte(strconv.FormatUint(2, 10))
	kv := &schema.KeyValue{
		Key:   k,
		Value: v,
	}
	if _, err = db.Set(kv); err != nil {
		log.Fatal(err)
	}
	db.Close()
	// Tampering
	opts := badger.DefaultOptions("test/test").WithLogger(nil)
	dbb, err := badger.OpenManaged(opts)
	if err != nil {
		log.Fatal(err)
	}
	txn := dbb.NewTransactionAt(math.MaxUint64, true)
	defer txn.Discard()

	item, err := txn.Get(treeKey(0, 0))
	if err != nil {
		log.Fatal(err)
	}
	ts := item.Version()
	v1 := []byte(strconv.FormatUint(3, 10))
	if err := txn.Set(item.Key(), v1); err != nil {
		log.Fatal(err)
	}
	if err := txn.CommitAt(ts, nil); err != nil {
		log.Fatal(err)
	}
	dbb.Close()
	// End Tampering
	db1, err := database.OpenDb(options, logger.NewSimpleLogger("immudb ", os.Stderr))
	assert.NoError(t, err)
	dbList.Append(db1)

	time.Sleep(500 * time.Millisecond)
	cco := CCOptions{}
	cco.iterationSleepTime = 1 * time.Millisecond
	cco.frequencySleepTime = 1 * time.Millisecond
	cco.singleiteration = true

	cc := NewCorruptionChecker(cco, dbList, &mockLogger{}, randomGenerator{})

	err = cc.Start(context.TODO())

	for i := 0; i < dbList.Length(); i++ {
		val := dbList.GetByIndex(int64(i))
		val.Close()
	}
	assert.Error(t, err)
}

func TestCorruptionCheckerOnTamperDb(t *testing.T) {
	var err error
	defer os.RemoveAll("test")
	dbList := NewDatabaseList()
	options := database.DefaultOption().WithDbName("test").WithDbRootPath("test")
	db, err := database.NewDb(options, logger.NewSimpleLogger("immudb ", os.Stderr))
	if err != nil {
		log.Fatal(err)
	}
	k := []byte(strconv.FormatUint(1, 10))
	v := []byte(strconv.FormatUint(2, 10))
	kv := &schema.KeyValue{
		Key:   k,
		Value: v,
	}
	if _, err = db.Set(kv); err != nil {
		log.Fatal(err)
	}
	k1 := []byte(strconv.FormatUint(3, 10))
	v1 := []byte(strconv.FormatUint(4, 10))
	kv1 := &schema.KeyValue{
		Key:   k1,
		Value: v1,
	}
	if _, err = db.Set(kv1); err != nil {
		log.Fatal(err)
	}
	db.Close()
	// Tampering
	opts := badger.DefaultOptions("test/test").WithLogger(nil)
	dbb, err := badger.OpenManaged(opts)
	if err != nil {
		log.Fatal(err)
	}
	txn := dbb.NewTransactionAt(math.MaxUint64, true)
	defer txn.Discard()

	item, err := txn.Get(treeKey(0, 0))
	if err != nil {
		log.Fatal(err)
	}
	ts := item.Version()
	v1 = []byte(`QWERTYUIOPASDFGHJKLZXCBVBN123456fake root`)
	if err := txn.Set(item.Key(), v1); err != nil {
		log.Fatal(err)
	}
	if err := txn.CommitAt(ts, nil); err != nil {
		log.Fatal(err)
	}
	dbb.Close()
	// End Tampering
	db1, err := database.OpenDb(options, logger.NewSimpleLogger("immudb ", os.Stderr))
	assert.NoError(t, err)
	dbList.Append(db1)

	time.Sleep(500 * time.Millisecond)
	cco := CCOptions{}
	cco.iterationSleepTime = 1 * time.Millisecond
	cco.frequencySleepTime = 1 * time.Millisecond
	cco.singleiteration = true

	cc := NewCorruptionChecker(cco, dbList, &mockLogger{}, randomGeneratorMock{})

	err = cc.Start(context.TODO())
	assert.NoError(t, err)

	for i := 0; i < dbList.Length(); i++ {
		val := dbList.GetByIndex(int64(i))
		val.Close()
	}
	assert.False(t, cc.GetStatus())
}

type randomGeneratorMock struct{}

func (rg randomGeneratorMock) getList(start, end uint64) []uint64 {
	ids := make([]uint64, 1)
	ids[0] = 1
	return ids
}

func TestCorruptionChecker_Stop(t *testing.T) {
	defer os.RemoveAll("test")
	dbList := NewDatabaseList()
	options := database.DefaultOption().WithDbName("test").WithDbRootPath("test")

	db1, _ := database.NewDb(options, logger.NewSimpleLogger("immudb ", os.Stderr))
	dbList.Append(db1)

	time.Sleep(500 * time.Millisecond)
	cco := CCOptions{}
	cco.iterationSleepTime = 1 * time.Millisecond
	cco.frequencySleepTime = 1 * time.Millisecond
	cco.singleiteration = true

	cc := NewCorruptionChecker(cco, dbList, &mockLogger{}, randomGenerator{})

	cc.Start(context.TODO())

	for i := 0; i < dbList.Length(); i++ {
		val := dbList.GetByIndex(int64(i))
		val.Close()
	}
	cc.Stop()
	assert.True(t, cc.GetStatus())
}

func TestCorruptionChecker_ExitImmediatly(t *testing.T) {
	var err error
	dbList := NewDatabaseList()
	db, _ := makeDb()
	kv := &schema.KeyValue{
		Key:   []byte(strconv.FormatUint(1, 10)),
		Value: []byte(strconv.FormatUint(2, 10)),
	}
	db.Set(kv)
	dbList.Append(db)

	time.Sleep(500 * time.Millisecond)
	cco := CCOptions{}
	cco.iterationSleepTime = 1 * time.Millisecond
	cco.frequencySleepTime = 1 * time.Millisecond
	cco.singleiteration = true

	cc := NewCorruptionChecker(cco, dbList, &mockLogger{}, randomGenerator{})
	err = cc.Start(context.TODO())
	cc.Stop()

	for i := 0; i < dbList.Length(); i++ {
		val := dbList.GetByIndex(int64(i))
		val.Close()
	}
	assert.Nil(t, err)
}

func treeKey(layer uint8, index uint64) []byte {
	k := make([]byte, 1+1+8)
	k[0] = 0
	k[1] = layer
	binary.BigEndian.PutUint64(k[2:], index)
	return k
}

func TestInt63(t *testing.T) {
	rand := newCryptoRandSource()
	n := rand.Int63()
	if n == 0 {
		t.Fatal("cryptorand source faild")
	}
}

func makeDB(dir string) *badger.DB {
	opts := badger.DefaultOptions(dir).
		WithLogger(nil)

	db, err := badger.OpenManaged(opts)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	return db
}

*/

type mockLogger struct{}

func (l *mockLogger) Errorf(f string, v ...interface{}) {}

func (l *mockLogger) Warningf(f string, v ...interface{}) {}

func (l *mockLogger) Infof(f string, v ...interface{}) {}

func (l *mockLogger) Debugf(f string, v ...interface{}) {}

func (l *mockLogger) CloneWithLevel(level logger.LogLevel) logger.Logger { return l }

/*
func TestCryptoRandSource_Seed(t *testing.T) {
	cs := newCryptoRandSource()
	cs.Seed(678)
}
*/

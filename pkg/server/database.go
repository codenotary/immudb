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
	"fmt"
	"os"
	"path/filepath"

	"github.com/codenotary/immudb/cmd/version"
	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/codenotary/immudb/pkg/logger"
	"github.com/codenotary/immudb/pkg/store"
	"github.com/dgraph-io/badger/v2/pb"
	"github.com/golang/protobuf/ptypes/empty"
)

//Db database instance
type Db struct {
	Store   *store.Store
	Logger  logger.Logger
	options *DbOptions
}

// OpenDb Opens an existing Database from disk
func OpenDb(op *DbOptions, log logger.Logger) (*Db, error) {
	var err error

	db := &Db{
		Logger:  log,
		options: op,
	}

	dbDir := filepath.Join(op.GetDbRootPath(), op.GetDbName())
	_, dbErr := os.Stat(dbDir)

	if os.IsNotExist(dbErr) {
		return nil, fmt.Errorf("Missing database directories")
	}

	db.Store, err = store.Open(store.DefaultOptions(dbDir, db.Logger))

	return db, logErr(db.Logger, "Unable to open store: %s", err)
}

// NewDb Creates a new Database along with it's directories and files
func NewDb(op *DbOptions, log logger.Logger) (*Db, error) {
	var err error

	db := &Db{
		Logger:  log,
		options: op,
	}

	if op.GetInMemoryStore() {
		db.Logger.Infof("Starting with in memory store")
		storeOpts, badgerOpts := store.DefaultOptions("", db.Logger)
		badgerOpts = badgerOpts.WithInMemory(true)
		db.Store, err = store.Open(storeOpts, badgerOpts)
		return db, logErr(db.Logger, "Unable to open store: %s", err)
	}

	dbDir := filepath.Join(op.GetDbRootPath(), op.GetDbName())

	if _, dbErr := os.Stat(dbDir); dbErr == nil {
		return nil, fmt.Errorf("Database directories already exist")
	}

	if err = os.MkdirAll(dbDir, os.ModePerm); err != nil {
		return nil, logErr(db.Logger, "Unable to create data folder: %s", err)
	}

	db.Store, err = store.Open(store.DefaultOptions(dbDir, db.Logger))
	return db, logErr(db.Logger, "Unable to open store: %s", err)
}

//Set ...
func (d *Db) Set(kv *schema.KeyValue) (*schema.Index, error) {
	return d.Store.Set(*kv)
}

//Get ...
func (d *Db) Get(k *schema.Key) (*schema.Item, error) {
	item, err := d.Store.Get(*k)
	if item == nil {
		d.Logger.Debugf("get %s: item not found", k.Key)
	} else {
		d.Logger.Debugf("get %s %d bytes", k.Key, len(item.Value))
	}
	return item, err
}

// CurrentRoot ...
func (d *Db) CurrentRoot(e *empty.Empty) (*schema.Root, error) {
	root, err := d.Store.CurrentRoot()
	if root != nil {
		d.Logger.Debugf("current root: %d %x", root.GetIndex(), root.GetRoot())
	}
	return root, err
}

//SafeSet ...
func (d *Db) SafeSet(opts *schema.SafeSetOptions) (*schema.Proof, error) {
	return d.Store.SafeSet(*opts)
}

//SafeGet ...
func (d *Db) SafeGet(opts *schema.SafeGetOptions) (*schema.SafeItem, error) {
	return d.Store.SafeGet(*opts)
}

// SetBatch ...
func (d *Db) SetBatch(kvl *schema.KVList) (*schema.Index, error) {
	return d.Store.SetBatch(*kvl)
}

//GetBatch ...
func (d *Db) GetBatch(kl *schema.KeyList) (*schema.ItemList, error) {
	list := &schema.ItemList{}
	for _, key := range kl.Keys {
		item, err := d.Store.Get(*key)
		if err == nil || err == store.ErrKeyNotFound {
			if item != nil {
				list.Items = append(list.Items, item)
			}
		} else {
			return nil, err
		}
	}
	return list, nil
}

// ExecAllOps ...
func (d *Db) ExecAllOps(operations *schema.Ops) (*schema.Index, error) {
	return d.Store.ExecAllOps(operations)
}

//Count ...
func (d *Db) Count(prefix *schema.KeyPrefix) (*schema.ItemsCount, error) {
	return d.Store.Count(*prefix)
}

// CountAll ...
func (d *Db) CountAll() *schema.ItemsCount {
	return &schema.ItemsCount{Count: d.Store.CountAll()}
}

// Inclusion ...
func (d *Db) Inclusion(index *schema.Index) (*schema.InclusionProof, error) {
	return d.Store.InclusionProof(*index)
}

// Consistency ...
func (d *Db) Consistency(index *schema.Index) (*schema.ConsistencyProof, error) {
	return d.Store.ConsistencyProof(*index)
}

// ByIndex ...
func (d *Db) ByIndex(index *schema.Index) (*schema.Item, error) {
	return d.Store.ByIndex(*index)
}

//BySafeIndex ...
func (d *Db) BySafeIndex(sio *schema.SafeIndexOptions) (*schema.SafeItem, error) {
	return d.Store.BySafeIndex(*sio)
}

//History ...
func (d *Db) History(options *schema.HistoryOptions) (*schema.ItemList, error) {
	return d.Store.History(options)
}

//Health ...
func (d *Db) Health(*empty.Empty) (*schema.HealthResponse, error) {
	health := d.Store.HealthCheck()
	return &schema.HealthResponse{Status: health, Version: version.VersionStr()}, nil
}

//Reference ...
func (d *Db) Reference(refOpts *schema.ReferenceOptions) (index *schema.Index, err error) {
	d.Logger.Debugf("reference options: %v", refOpts)
	return d.Store.Reference(refOpts)
}

//Reference ...
func (d *Db) GetReference(refOpts *schema.Key) (index *schema.Item, err error) {
	d.Logger.Debugf("getReference options: %v", refOpts)
	return d.Store.GetReference(*refOpts)
}

//SafeReference ...
func (d *Db) SafeReference(safeRefOpts *schema.SafeReferenceOptions) (proof *schema.Proof, err error) {
	return d.Store.SafeReference(*safeRefOpts)
}

//ZAdd ...
func (d *Db) ZAdd(opts *schema.ZAddOptions) (*schema.Index, error) {
	return d.Store.ZAdd(*opts)
}

// ZScan ...
func (d *Db) ZScan(opts *schema.ZScanOptions) (*schema.ZItemList, error) {
	return d.Store.ZScan(*opts)
}

//SafeZAdd ...
func (d *Db) SafeZAdd(opts *schema.SafeZAddOptions) (*schema.Proof, error) {
	return d.Store.SafeZAdd(*opts)
}

//Scan ...
func (d *Db) Scan(opts *schema.ScanOptions) (*schema.ItemList, error) {
	return d.Store.Scan(*opts)
}

//IScan ...
func (d *Db) IScan(opts *schema.IScanOptions) (*schema.Page, error) {
	return d.Store.IScan(*opts)
}

//Dump ...
func (d *Db) Dump(in *empty.Empty, stream schema.ImmuService_DumpServer) error {
	kvChan := make(chan *pb.KVList)
	done := make(chan bool)

	retrieveLists := func() {
		for {
			list, more := <-kvChan
			if more {
				stream.Send(list)
			} else {
				done <- true
				return
			}
		}
	}

	go retrieveLists()
	err := d.Store.Dump(kvChan)
	<-done

	d.Logger.Debugf("Dump stream complete")
	return err
}

// PrintTree ...
func (d *Db) PrintTree() *schema.Tree {
	return d.Store.GetTree()
}

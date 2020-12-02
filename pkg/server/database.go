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
	"time"

	"github.com/codenotary/immudb/embedded/store"

	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/codenotary/immudb/pkg/logger"
	"github.com/golang/protobuf/ptypes/empty"
)

//Db database instance
type Db struct {
	Store *store.ImmuStore
	tx    *store.Tx

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

	indexOptions := store.DefaultIndexOptions().WithRenewSnapRootAfter(0)

	db.Store, err = store.Open(dbDir, store.DefaultOptions().WithIndexOptions(indexOptions))
	if err != nil {
		return nil, logErr(db.Logger, "Unable to open store: %s", err)
	}

	db.tx = db.Store.NewTx()

	return db, nil
}

// NewDb Creates a new Database along with it's directories and files
func NewDb(op *DbOptions, log logger.Logger) (*Db, error) {
	var err error

	db := &Db{
		Logger:  log,
		options: op,
	}

	dbDir := filepath.Join(op.GetDbRootPath(), op.GetDbName())

	if _, dbErr := os.Stat(dbDir); dbErr == nil {
		return nil, fmt.Errorf("Database directories already exist")
	}

	if err = os.MkdirAll(dbDir, os.ModePerm); err != nil {
		return nil, logErr(db.Logger, "Unable to create data folder: %s", err)
	}

	indexOptions := store.DefaultIndexOptions().WithRenewSnapRootAfter(0)

	db.Store, err = store.Open(dbDir, store.DefaultOptions().WithIndexOptions(indexOptions))
	if err != nil {
		return nil, logErr(db.Logger, "Unable to open store: %s", err)
	}

	db.tx = db.Store.NewTx()

	return db, nil
}

//Set ...
func (d *Db) Set(kv *schema.KeyValue) (*schema.Index, error) {
	if kv == nil {
		return nil, store.ErrIllegalArguments
	}

	id, _, _, err := d.Store.Commit([]*store.KV{{Key: kv.Key, Value: kv.Value}})
	if err != nil {
		return nil, fmt.Errorf("unexpected error %v during %s", err, "Set")
	}

	return &schema.Index{Index: id - 1}, nil
}

//Get ...
func (d *Db) Get(k *schema.Key) (*schema.Item, error) {
	return d.GetSince(k, 0)
}

func (d *Db) GetSince(k *schema.Key, ts uint64) (*schema.Item, error) {
	for {
		its, err := d.Store.IndexInfo()
		if err != nil {
			return nil, err
		}

		if its >= ts {
			break
		}

		time.Sleep(time.Duration(5) * time.Millisecond)
	}

	snapshot, err := d.Store.Snapshot()
	if err != nil {
		return nil, err
	}
	defer snapshot.Close()

	_, id, err := snapshot.Get(k.Key)
	if err != nil {
		return nil, err
	}

	d.Store.ReadTx(id, d.tx)

	val, err := d.Store.ReadValue(d.tx, k.Key)
	if err != nil {
		return nil, err
	}

	return &schema.Item{Key: k.Key, Value: val, Index: id}, err
}

// CurrentRoot ...
func (d *Db) CurrentRoot() (*schema.Root, error) {
	id, alh := d.Store.Alh()

	return &schema.Root{Payload: &schema.RootIndex{Index: id, Root: alh[:]}}, nil
}

//SafeSet ...
func (d *Db) SafeSet(opts *schema.SafeSetOptions) (*schema.Proof, error) {
	//return d.Store.SafeSet(*opts)
	return nil, fmt.Errorf("Functionality not yet supported: %s", "SafeSet")
}

//SafeGet ...
func (d *Db) SafeGet(opts *schema.SafeGetOptions) (*schema.SafeItem, error) {
	//return d.Store.SafeGet(*opts)
	return nil, fmt.Errorf("Functionality not yet supported: %s", "SafeGet")
}

// SetBatch ...
func (d *Db) SetBatch(kvl *schema.KVList) (*schema.Index, error) {
	if kvl == nil {
		return nil, store.ErrIllegalArguments
	}

	entries := make([]*store.KV, len(kvl.KVs))

	id, _, _, err := d.Store.Commit(entries)
	if err != nil {
		return nil, err
	}

	return &schema.Index{Index: id - 1}, nil
}

//GetBatch ...
func (d *Db) GetBatch(kl *schema.KeyList) (*schema.ItemList, error) {
	/*
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
	*/
	return nil, fmt.Errorf("Functionality not yet supported: %s", "GetBatch")
}

// ExecAllOps ...
func (d *Db) ExecAllOps(operations *schema.Ops) (*schema.Index, error) {
	//return d.Store.ExecAllOps(operations)
	return nil, fmt.Errorf("Functionality not yet supported: %s", "ExecAllOps")
}

//Size ...
func (d *Db) Size() (uint64, error) {
	return d.Store.TxCount(), nil
}

//Count ...
func (d *Db) Count(prefix *schema.KeyPrefix) (*schema.ItemsCount, error) {
	//return d.Store.Count(*prefix)
	return nil, fmt.Errorf("Functionality not yet supported: %s", "Count")
}

// CountAll ...
func (d *Db) CountAll() *schema.ItemsCount {
	//return &schema.ItemsCount{Count: d.Store.CountAll()}
	return nil
}

// Inclusion ...
func (d *Db) Inclusion(index *schema.Index) (*schema.InclusionProof, error) {
	//return d.Store.InclusionProof(*index)
	return nil, fmt.Errorf("Functionality not yet supported: %s", "Inclusion")
}

// Consistency ...
func (d *Db) Consistency(index *schema.Index) (*schema.ConsistencyProof, error) {
	//return d.Store.ConsistencyProof(*index)
	return nil, fmt.Errorf("Functionality not yet supported: %s", "Consistency")
}

// ByIndex ...
func (d *Db) ByIndex(index *schema.Index) (*schema.Item, error) {
	//return d.Store.ByIndex(*index)
	return nil, fmt.Errorf("Functionality not yet supported: %s", "ByIndex")
}

//BySafeIndex ...
func (d *Db) BySafeIndex(sio *schema.SafeIndexOptions) (*schema.SafeItem, error) {
	//return d.Store.BySafeIndex(*sio)
	return nil, fmt.Errorf("Functionality not yet supported: %s", "BySafeIndex")
}

//History ...
func (d *Db) History(options *schema.HistoryOptions) (*schema.ItemList, error) {
	//return d.Store.History(options)
	return nil, fmt.Errorf("Functionality not yet supported: %s", "History")
}

//Health ...
func (d *Db) Health(*empty.Empty) (*schema.HealthResponse, error) {
	return &schema.HealthResponse{Status: true, Version: fmt.Sprintf("%d", store.Version)}, nil
}

//Reference ...
func (d *Db) Reference(refOpts *schema.ReferenceOptions) (index *schema.Index, err error) {
	/*
		d.Logger.Debugf("reference options: %v", refOpts)
		return d.Store.Reference(refOpts)
	*/
	return nil, fmt.Errorf("Functionality not yet supported: %s", "Reference")
}

//SafeReference ...
func (d *Db) SafeReference(safeRefOpts *schema.SafeReferenceOptions) (proof *schema.Proof, err error) {
	//return d.Store.SafeReference(*safeRefOpts)
	return nil, fmt.Errorf("Functionality not yet supported: %s", "SafeReference")
}

//ZAdd ...
func (d *Db) ZAdd(opts *schema.ZAddOptions) (*schema.Index, error) {
	//return d.Store.ZAdd(*opts)
	return nil, fmt.Errorf("Functionality not yet supported: %s", "ZAdd")
}

// ZScan ...
func (d *Db) ZScan(opts *schema.ZScanOptions) (*schema.ZItemList, error) {
	//return d.Store.ZScan(*opts)
	return nil, fmt.Errorf("Functionality not yet supported: %s", "ZScan")
}

//SafeZAdd ...
func (d *Db) SafeZAdd(opts *schema.SafeZAddOptions) (*schema.Proof, error) {
	//return d.Store.SafeZAdd(*opts)
	return nil, fmt.Errorf("Functionality not yet supported: %s", "SafeZAdd")
}

//Scan ...
func (d *Db) Scan(opts *schema.ScanOptions) (*schema.ItemList, error) {
	//return d.Store.Scan(*opts)
	return nil, fmt.Errorf("Functionality not yet supported: %s", "Scan")
}

//IScan ...
func (d *Db) IScan(opts *schema.IScanOptions) (*schema.Page, error) {
	//return d.Store.IScan(*opts)
	return nil, fmt.Errorf("Functionality not yet supported: %s", "IScan")
}

//Dump ...
func (d *Db) Dump(in *empty.Empty, stream schema.ImmuService_DumpServer) error {
	/*
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
	*/
	return fmt.Errorf("Functionality not yet supported: %s", "Dump")
}

// PrintTree ...
func (d *Db) PrintTree() *schema.Tree {
	//return d.Store.GetTree()
	return nil
}

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
	"fmt"
	"os"
	"path/filepath"

	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/codenotary/immudb/pkg/logger"
	"github.com/codenotary/immudb/pkg/store"
	"github.com/dgraph-io/badger/v2/pb"
	"github.com/golang/protobuf/ptypes/empty"
)

type Db struct {
	Store    *store.Store
	SysStore *store.Store
	Logger   logger.Logger
	options  *DbOptions
}

// OpenDb Opens an existing Database from disk
func OpenDb(op *DbOptions) (*Db, error) {
	var err error
	db := &Db{
		Logger:  logger.NewSimpleLogger(op.GetDbName(), os.Stderr),
		options: op,
	}
	sysDbDir := filepath.Join(op.GetDbRootPath(), op.GetDbName(), op.GetSysDbDir())
	dbDir := filepath.Join(op.GetDbRootPath(), op.GetDbName(), op.GetDbDir())
	_, sysDbErr := os.Stat(sysDbDir)
	_, dbErr := os.Stat(dbDir)

	if os.IsNotExist(sysDbErr) || os.IsNotExist(dbErr) {
		return nil, fmt.Errorf("Missing database directories")
	}
	db.SysStore, err = store.Open(store.DefaultOptions(sysDbDir, db.Logger))
	if err != nil {
		db.Logger.Errorf("Unable to open sysstore: %s", err)
		return nil, err
	}
	db.Store, err = store.Open(store.DefaultOptions(dbDir, db.Logger))
	if err != nil {
		db.Logger.Errorf("Unable to open store: %s", err)
		return nil, err
	}
	return db, nil
}

// NewDb Creates a new Database along with it's directories and files
func NewDb(op *DbOptions) (*Db, error) {
	var err error
	db := &Db{
		Logger:  logger.NewSimpleLogger(op.GetDbName()+" ", os.Stderr),
		options: op,
	}
	sysDbDir := filepath.Join(op.GetDbRootPath(), op.GetDbName(), op.GetSysDbDir())
	dbDir := filepath.Join(op.GetDbRootPath(), op.GetDbName(), op.GetDbDir())
	_, sysDbErr := os.Stat(sysDbDir)
	_, dbErr := os.Stat(dbDir)
	if os.IsExist(sysDbErr) || os.IsExist(dbErr) {
		return nil, fmt.Errorf("Database directories already exist")
	}
	if err = os.MkdirAll(sysDbDir, os.ModePerm); err != nil {
		db.Logger.Errorf("Unable to create sys data folder: %s", err)
		return nil, err
	}

	if err = os.MkdirAll(dbDir, os.ModePerm); err != nil {
		db.Logger.Errorf("Unable to create data folder: %s", err)
		return nil, err
	}
	db.SysStore, err = store.Open(store.DefaultOptions(sysDbDir, db.Logger))
	if err != nil {
		db.Logger.Errorf("Unable to open sysstore: %s", err)
		return nil, err
	}
	db.Store, err = store.Open(store.DefaultOptions(dbDir, db.Logger))
	if err != nil {
		db.Logger.Errorf("Unable to open store: %s", err)
		return nil, err
	}
	return db, nil
}

func (d *Db) Set(ctx context.Context, kv *schema.KeyValue) (*schema.Index, error) {
	d.Logger.Debugf("set %s %d bytes", kv.Key, len(kv.Value))
	item, err := d.Store.Set(*kv)
	if err != nil {
		return nil, err
	}
	return item, nil
}

func (d *Db) Get(ctx context.Context, k *schema.Key) (*schema.Item, error) {
	item, err := d.Store.Get(*k)
	if item == nil {
		d.Logger.Debugf("get %s: item not found", k.Key)
	} else {
		d.Logger.Debugf("get %s %d bytes", k.Key, len(item.Value))
	}
	if err != nil {
		return nil, err
	}
	return item, nil
}
func (d *Db) CurrentRoot(ctx context.Context, e *empty.Empty) (*schema.Root, error) {
	root, err := d.Store.CurrentRoot()
	if root != nil {
		d.Logger.Debugf("current root: %d %x", root.Index, root.Root)
	}
	return root, err
}

func (d *Db) SetSV(ctx context.Context, skv *schema.StructuredKeyValue) (*schema.Index, error) {
	kv, err := skv.ToKV()
	if err != nil {
		return nil, err
	}
	return d.Set(ctx, kv)
}
func (d *Db) GetSV(ctx context.Context, k *schema.Key) (*schema.StructuredItem, error) {
	it, err := d.Get(ctx, k)
	si, err := it.ToSItem()
	if err != nil {
		return nil, err
	}
	return si, err
}

func (d *Db) SafeSet(ctx context.Context, opts *schema.SafeSetOptions) (*schema.Proof, error) {
	d.Logger.Debugf("safeset %s %d bytes", opts.Kv.Key, len(opts.Kv.Value))
	item, err := d.Store.SafeSet(*opts)
	if err != nil {
		return nil, err
	}
	return item, nil
}
func (d *Db) SafeGet(ctx context.Context, opts *schema.SafeGetOptions) (*schema.SafeItem, error) {
	d.Logger.Debugf("safeget %s", opts.Key)
	sitem, err := d.Store.SafeGet(*opts)
	if err != nil {
		return nil, err
	}
	return sitem, nil
}
func (d *Db) SafeSetSV(ctx context.Context, sopts *schema.SafeSetSVOptions) (*schema.Proof, error) {
	kv, err := sopts.Skv.ToKV()
	if err != nil {
		return nil, err
	}
	opts := &schema.SafeSetOptions{
		Kv:        kv,
		RootIndex: sopts.RootIndex,
	}
	return d.SafeSet(ctx, opts)
}
func (d *Db) SafeGetSV(ctx context.Context, opts *schema.SafeGetOptions) (*schema.SafeStructuredItem, error) {
	it, err := d.SafeGet(ctx, opts)
	ssitem, err := it.ToSafeSItem()
	if err != nil {
		return nil, err
	}
	return ssitem, err
}

func (d *Db) SetBatch(ctx context.Context, kvl *schema.KVList) (*schema.Index, error) {
	d.Logger.Debugf("set batch %d", len(kvl.KVs))
	index, err := d.Store.SetBatch(*kvl)
	if err != nil {
		return nil, err
	}
	return index, nil
}

func (d *Db) GetBatch(ctx context.Context, kl *schema.KeyList) (*schema.ItemList, error) {
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

func (d *Db) SetBatchSV(ctx context.Context, skvl *schema.SKVList) (*schema.Index, error) {
	kvl, err := skvl.ToKVList()
	if err != nil {
		return nil, err
	}
	return d.SetBatch(ctx, kvl)
}
func (d *Db) GetBatchSV(ctx context.Context, kl *schema.KeyList) (*schema.StructuredItemList, error) {
	list, err := d.GetBatch(ctx, kl)
	slist, err := list.ToSItemList()
	if err != nil {
		return nil, err
	}
	return slist, err
}

func (d *Db) ScanSV(ctx context.Context, opts *schema.ScanOptions) (*schema.StructuredItemList, error) {
	d.Logger.Debugf("scan %+v", *opts)
	list, err := d.Store.Scan(*opts)
	slist, err := list.ToSItemList()
	if err != nil {
		return nil, err
	}
	return slist, err
}

func (d *Db) Count(ctx context.Context, prefix *schema.KeyPrefix) (*schema.ItemsCount, error) {
	d.Logger.Debugf("count %s", prefix.Prefix)
	return d.Store.Count(*prefix)
}

func (d *Db) Inclusion(ctx context.Context, index *schema.Index) (*schema.InclusionProof, error) {
	d.Logger.Debugf("inclusion for index %d ", index.Index)
	proof, err := d.Store.InclusionProof(*index)
	if err != nil {
		return nil, err
	}
	return proof, nil
}

func (d *Db) Consistency(ctx context.Context, index *schema.Index) (*schema.ConsistencyProof, error) {
	d.Logger.Debugf("consistency for index %d ", index.Index)
	proof, err := d.Store.ConsistencyProof(*index)
	if err != nil {
		return nil, err
	}
	return proof, nil
}

func (d *Db) ByIndex(ctx context.Context, index *schema.Index) (*schema.Item, error) {
	d.Logger.Debugf("get by index %d ", index.Index)
	item, err := d.Store.ByIndex(*index)
	if err != nil {
		return nil, err
	}
	return item, nil
}

func (d *Db) ByIndexSV(ctx context.Context, index *schema.Index) (*schema.StructuredItem, error) {
	d.Logger.Debugf("get by index %d ", index.Index)
	item, err := d.Store.ByIndex(*index)
	if err != nil {
		return nil, err
	}
	sitem, err := item.ToSItem()
	if err != nil {
		return nil, err
	}
	return sitem, nil
}

func (d *Db) BySafeIndex(ctx context.Context, sio *schema.SafeIndexOptions) (*schema.SafeItem, error) {
	d.Logger.Debugf("get by safeIndex %d ", sio.Index)
	item, err := d.Store.BySafeIndex(*sio)
	if err != nil {
		return nil, err
	}
	return item, nil
}

func (d *Db) History(ctx context.Context, key *schema.Key) (*schema.ItemList, error) {
	d.Logger.Debugf("history for key %s ", string(key.Key))
	list, err := d.Store.History(*key)
	if err != nil {
		return nil, err
	}
	return list, nil
}

func (d *Db) HistorySV(ctx context.Context, key *schema.Key) (*schema.StructuredItemList, error) {
	d.Logger.Debugf("history for key %s ", string(key.Key))

	list, err := d.Store.History(*key)
	if err != nil {
		return nil, err
	}

	slist, err := list.ToSItemList()
	if err != nil {
		return nil, err
	}
	return slist, err
}

func (d *Db) Health(context.Context, *empty.Empty) (*schema.HealthResponse, error) {
	health := d.Store.HealthCheck()
	d.Logger.Debugf("health check: %v", health)
	return &schema.HealthResponse{Status: health}, nil
}

func (d *Db) Reference(ctx context.Context, refOpts *schema.ReferenceOptions) (index *schema.Index, err error) {
	index, err = d.Store.Reference(refOpts)
	if err != nil {
		return nil, err
	}
	d.Logger.Debugf("reference options: %v", refOpts)
	return index, nil
}

func (d *Db) SafeReference(ctx context.Context, safeRefOpts *schema.SafeReferenceOptions) (proof *schema.Proof, err error) {
	proof, err = d.Store.SafeReference(*safeRefOpts)
	if err != nil {
		return nil, err
	}
	d.Logger.Debugf("safe reference options: %v", safeRefOpts)
	return proof, nil
}

func (d *Db) ZAdd(ctx context.Context, opts *schema.ZAddOptions) (*schema.Index, error) {
	d.Logger.Debugf("zadd %+v", *opts)
	return d.Store.ZAdd(*opts)
}

func (d *Db) ZScan(ctx context.Context, opts *schema.ZScanOptions) (*schema.ItemList, error) {
	d.Logger.Debugf("zscan %+v", *opts)
	return d.Store.ZScan(*opts)
}

func (d *Db) ZScanSV(ctx context.Context, opts *schema.ZScanOptions) (*schema.StructuredItemList, error) {
	d.Logger.Debugf("zscan %+v", *opts)
	list, err := d.Store.ZScan(*opts)
	slist, err := list.ToSItemList()
	if err != nil {
		return nil, err
	}
	return slist, err
}

func (d *Db) SafeZAdd(ctx context.Context, opts *schema.SafeZAddOptions) (*schema.Proof, error) {
	d.Logger.Debugf("zadd %+v", *opts)
	return d.Store.SafeZAdd(*opts)
}

func (d *Db) IScan(ctx context.Context, opts *schema.IScanOptions) (*schema.Page, error) {
	d.Logger.Debugf("iscan %+v", *opts)
	return d.Store.IScan(*opts)
}

func (d *Db) IScanSV(ctx context.Context, opts *schema.IScanOptions) (*schema.SPage, error) {
	d.Logger.Debugf("zscan %+v", *opts)
	page, err := d.Store.IScan(*opts)
	SPage, err := page.ToSPage()
	if err != nil {
		return nil, err
	}
	return SPage, err
}

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

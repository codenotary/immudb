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

package db

import (
	"context"
	"os"
	"path/filepath"

	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/codenotary/immudb/pkg/logger"
	"github.com/codenotary/immudb/pkg/store"
	"github.com/golang/protobuf/ptypes/empty"
)

type Db struct {
	Store    *store.Store
	SysStore *store.Store
	Logger   logger.Logger
	options  *DbOptions
}

func NewDb(op *DbOptions) (*Db, error) {
	var err error
	db := &Db{
		Logger:  logger.NewSimpleLogger(op.DbName+" ", os.Stderr),
		options: op,
	}
	sysDbDir := filepath.Join(op.DbName, op.GetSysDbDir())
	if err = os.MkdirAll(sysDbDir, os.ModePerm); err != nil {
		db.Logger.Errorf("Unable to create sys data folder: %s", err)
		return nil, err
	}
	dbDir := filepath.Join(op.DbName, op.GetDbDir())
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

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

package database

import (
	"bytes"
	"fmt"
	"github.com/codenotary/immudb/pkg/common"
	"math"
	"os"
	"path/filepath"
	"time"

	"github.com/codenotary/immudb/embedded/store"

	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/codenotary/immudb/pkg/logger"
	"github.com/golang/protobuf/ptypes/empty"
)

type Db interface {
	Set(kv *schema.KeyValue) (*schema.Root, error)
	Get(k *schema.Key) (*schema.Item, error)
	GetSince(k *schema.Key, index uint64) (*schema.Item, error)
	CurrentRoot() (*schema.Root, error)
	SafeSet(opts *schema.SafeSetOptions) (*schema.Proof, error)
	SafeGet(opts *schema.SafeGetOptions) (*schema.SafeItem, error)
	SetBatch(kvl *schema.KVList) (*schema.Root, error)
	GetBatch(kl *schema.KeyList) (*schema.ItemList, error)
	ExecAllOps(operations *schema.Ops) (*schema.Root, error)
	Size() (uint64, error)
	Count(prefix *schema.KeyPrefix) (*schema.ItemsCount, error)
	CountAll() *schema.ItemsCount
	ByIndex(index *schema.Index) (*schema.Tx, error)
	BySafeIndex(sio *schema.SafeIndexOptions) (*schema.VerifiedTx, error)
	History(options *schema.HistoryOptions) (*schema.ItemList, error)
	Health(*empty.Empty) (*schema.HealthResponse, error)
	Reference(refOpts *schema.ReferenceOptions) (index *schema.Root, err error)
	GetReference(refOpts *schema.Key) (item *schema.Item, err error)
	SafeReference(safeRefOpts *schema.SafeReferenceOptions) (proof *schema.Proof, err error)
	ZAdd(opts *schema.ZAddOptions) (*schema.Root, error)
	ZScan(opts *schema.ZScanOptions) (*schema.ZItemList, error)
	SafeZAdd(opts *schema.SafeZAddOptions) (*schema.Proof, error)
	Scan(opts *schema.ScanOptions) (*schema.ItemList, error)
	IScan(opts *schema.IScanOptions) (*schema.Page, error)
	Dump(in *empty.Empty, stream schema.ImmuService_DumpServer) error
	PrintTree() (*schema.Tree, error)
	Close() error
	GetOptions() *DbOptions
}

//Db database instance
type db struct {
	st *store.ImmuStore

	tx *store.Tx

	Logger  logger.Logger
	options *DbOptions
}

// OpenDb Opens an existing Database from disk
func OpenDb(op *DbOptions, log logger.Logger) (*db, error) {
	var err error

	db := &db{
		Logger:  log,
		options: op,
	}

	dbDir := filepath.Join(op.GetDbRootPath(), op.GetDbName())

	_, dbErr := os.Stat(dbDir)
	if os.IsNotExist(dbErr) {
		return nil, fmt.Errorf("Missing database directories")
	}

	indexOptions := store.DefaultIndexOptions().WithRenewSnapRootAfter(0)

	db.st, err = store.Open(dbDir, store.DefaultOptions().WithIndexOptions(indexOptions))
	if err != nil {
		return nil, logErr(db.Logger, "Unable to open store: %s", err)
	}

	db.tx = db.st.NewTx()

	return db, nil
}

// NewDb Creates a new Database along with it's directories and files
func NewDb(op *DbOptions, log logger.Logger) (*db, error) {
	var err error

	db := &db{
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
	storeOpts := store.DefaultOptions().WithIndexOptions(indexOptions).WithMaxLinearProofLen(0)

	db.st, err = store.Open(dbDir, storeOpts)
	if err != nil {
		return nil, logErr(db.Logger, "Unable to open store: %s", err)
	}

	db.tx = db.st.NewTx()

	return db, nil
}

//Set ...
func (d *db) Set(kv *schema.KeyValue) (*schema.Root, error) {
	if kv == nil {
		return nil, store.ErrIllegalArguments
	}

	id, _, alh, err := d.st.Commit([]*store.KV{{Key: kv.Key, Value: kv.Value}})
	if err != nil {
		return nil, fmt.Errorf("unexpected error %v during %s", err, "Set")
	}

	return &schema.Root{
		Payload: &schema.RootIndex{
			Index: id,
			Root:  alh[:],
		},
	}, nil
}

//Get ...
func (d *db) Get(k *schema.Key) (*schema.Item, error) {
	item, err := d.GetSince(k, 0)
	if err != nil {
		return nil, err
	}
	//Reference lookup
	if bytes.HasPrefix(item.Value, common.ReferencePrefix) {
		ref := bytes.TrimPrefix(item.Value, common.ReferencePrefix)
		key, _, _ := common.UnwrapIndexReference(ref)
		return d.GetSince(&schema.Key{Key: key}, 0)
	}
	return item, err
}

func (d *db) waitForIndexing(ts uint64) error {
	for {
		its, err := d.st.IndexInfo()
		if err != nil {
			return err
		}

		if its >= ts {
			break
		}

		time.Sleep(time.Duration(5) * time.Millisecond)
	}
	return nil
}

func (d *db) GetSince(k *schema.Key, ts uint64) (*schema.Item, error) {
	err := d.waitForIndexing(ts)
	if err != nil {
		return nil, err
	}

	snapshot, err := d.st.Snapshot()
	if err != nil {
		return nil, err
	}
	defer snapshot.Close()

	_, id, err := snapshot.Get(k.Key)
	if err != nil {
		return nil, err
	}

	d.st.ReadTx(id, d.tx)

	val, err := d.st.ReadValue(d.tx, k.Key)
	if err != nil {
		return nil, err
	}

	return &schema.Item{Key: k.Key, Value: val, Index: id}, err
}

// CurrentRoot ...
func (d *db) CurrentRoot() (*schema.Root, error) {
	id, alh := d.st.Alh()

	return &schema.Root{Payload: &schema.RootIndex{Index: id, Root: alh[:]}}, nil
}

//SafeSet ...
func (d *db) SafeSet(opts *schema.SafeSetOptions) (*schema.Proof, error) {
	if opts == nil {
		return nil, store.ErrIllegalArguments
	}

	root, err := d.Set(opts.Kv)
	if err != nil {
		return nil, err
	}

	// key-value inclusion proof
	err = d.st.ReadTx(root.Payload.Index, d.tx)
	if err != nil {
		return nil, err
	}

	inclusionProof, err := d.tx.Proof(opts.Kv.Key)
	if err != nil {
		return nil, err
	}

	proof := &schema.Proof{
		InclusionProof: inclusionProofTo(inclusionProof),
		DualProof:      nil,
	}

	var rootTx *store.Tx

	if opts.RootIndex.Index == 0 {
		rootTx = d.tx
	} else {
		rootTx = d.st.NewTx()

		err = d.st.ReadTx(opts.RootIndex.Index, rootTx)
		if err != nil {
			return nil, err
		}
	}

	var sourceTx, targetTx *store.Tx

	if opts.RootIndex.Index <= root.Payload.Index {
		sourceTx = rootTx
		targetTx = d.tx
	} else {
		sourceTx = d.tx
		targetTx = rootTx
	}

	dualProof, err := d.st.DualProof(sourceTx, targetTx)
	if err != nil {
		return nil, err
	}

	proof.DualProof = dualProofTo(dualProof)

	return proof, nil
}

//SafeGet ...
func (d *db) SafeGet(opts *schema.SafeGetOptions) (*schema.SafeItem, error) {
	if opts == nil || opts.RootIndex == nil {
		return nil, store.ErrIllegalArguments
	}

	// get value of key
	it, err := d.Get(&schema.Key{Key: opts.Key})
	if err != nil {
		return nil, err
	}

	// key-value inclusion proof
	err = d.st.ReadTx(it.Index, d.tx)
	if err != nil {
		return nil, err
	}

	inclusionProof, err := d.tx.Proof(opts.Key)
	if err != nil {
		return nil, err
	}

	proof := &schema.Proof{
		InclusionProof: inclusionProofTo(inclusionProof),
		DualProof:      nil,
	}

	var rootTx *store.Tx

	if opts.RootIndex.Index == 0 {
		rootTx = d.tx
	} else {
		rootTx = d.st.NewTx()

		err = d.st.ReadTx(opts.RootIndex.Index, rootTx)
		if err != nil {
			return nil, err
		}
	}

	var sourceTx, targetTx *store.Tx

	if opts.RootIndex.Index <= it.Index {
		sourceTx = rootTx
		targetTx = d.tx
	} else {
		sourceTx = d.tx
		targetTx = rootTx
	}

	dualProof, err := d.st.DualProof(sourceTx, targetTx)
	if err != nil {
		return nil, err
	}

	proof.DualProof = dualProofTo(dualProof)

	return &schema.SafeItem{Item: it, Proof: proof}, nil
}

// SetBatch ...
func (d *db) SetBatch(kvl *schema.KVList) (*schema.Root, error) {
	if kvl == nil {
		return nil, store.ErrIllegalArguments
	}

	entries := make([]*store.KV, len(kvl.KVs))

	for i, kv := range kvl.KVs {
		entries[i] = &store.KV{Key: kv.Key, Value: kv.Value}
	}

	id, _, alh, err := d.st.Commit(entries)
	if err != nil {
		return nil, err
	}

	return &schema.Root{
		Payload: &schema.RootIndex{
			Index: id,
			Root:  alh[:],
		},
	}, nil
}

//GetBatch ...
func (d *db) GetBatch(kl *schema.KeyList) (*schema.ItemList, error) {
	list := &schema.ItemList{}
	for _, key := range kl.Keys {
		item, err := d.Get(key)
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
func (d *db) ExecAllOps(operations *schema.Ops) (*schema.Root, error) {
	//return d.st.ExecAllOps(operations)
	return nil, fmt.Errorf("Functionality not yet supported: %s", "ExecAllOps")
}

//Size ...
func (d *db) Size() (uint64, error) {
	return d.st.TxCount(), nil
}

//Count ...
func (d *db) Count(prefix *schema.KeyPrefix) (*schema.ItemsCount, error) {
	//return d.st.Count(*prefix)
	return nil, fmt.Errorf("Functionality not yet supported: %s", "Count")
}

// CountAll ...
func (d *db) CountAll() *schema.ItemsCount {
	return &schema.ItemsCount{Count: d.st.TxCount()}
}

// ByIndex ...
func (d *db) ByIndex(index *schema.Index) (*schema.Tx, error) {
	if index == nil {
		return nil, store.ErrIllegalArguments
	}

	// key-value inclusion proof
	err := d.st.ReadTx(index.Index, d.tx)
	if err != nil {
		return nil, err
	}

	return txTo(d.tx), nil
}

//BySafeIndex ...
func (d *db) BySafeIndex(sio *schema.SafeIndexOptions) (*schema.VerifiedTx, error) {
	if sio == nil || sio.RootIndex == nil {
		return nil, store.ErrIllegalArguments
	}

	// key-value inclusion proof
	err := d.st.ReadTx(sio.Index, d.tx)
	if err != nil {
		return nil, err
	}

	var sourceTx, targetTx *store.Tx

	var rootTx *store.Tx

	if sio.RootIndex.Index == 0 {
		rootTx = d.tx
	} else {
		rootTx = d.st.NewTx()

		err = d.st.ReadTx(sio.RootIndex.Index, rootTx)
		if err != nil {
			return nil, err
		}
	}

	if sio.RootIndex.Index <= sio.Index {
		sourceTx = rootTx
		targetTx = d.tx
	} else {
		sourceTx = d.tx
		targetTx = rootTx
	}

	dualProof, err := d.st.DualProof(sourceTx, targetTx)
	if err != nil {
		return nil, err
	}

	return &schema.VerifiedTx{
		Tx:        txTo(d.tx),
		DualProof: dualProofTo(dualProof),
	}, nil
}

//History ...
func (d *db) History(options *schema.HistoryOptions) (*schema.ItemList, error) {
	snapshot, err := d.st.Snapshot()
	if err != nil {
		return nil, err
	}
	defer snapshot.Close()

	limit := math.MaxInt64
	if options.Limit > 0 {
		limit = int(options.Offset + options.Limit)
	}

	tss, err := snapshot.GetTs(options.Key, int64(limit))
	if err != nil {
		return nil, err
	}

	list := &schema.ItemList{}

	for i := int(options.Offset); i < len(tss); i++ {
		ts := tss[i]

		err = d.st.ReadTx(ts, d.tx)
		if err != nil {
			return nil, err
		}

		val, err := d.st.ReadValue(d.tx, options.Key)
		if err != nil {
			return nil, err
		}

		item := &schema.Item{Key: options.Key, Value: val, Index: ts}

		if options.Reverse {
			list.Items = append([]*schema.Item{item}, list.Items...)
		} else {
			list.Items = append(list.Items, item)
		}
	}

	return list, nil
}

//Health ...
func (d *db) Health(*empty.Empty) (*schema.HealthResponse, error) {
	return &schema.HealthResponse{Status: true, Version: fmt.Sprintf("%d", store.Version)}, nil
}

//Scan ...
func (d *db) Scan(opts *schema.ScanOptions) (*schema.ItemList, error) {
	//return d.st.Scan(*opts)
	return nil, fmt.Errorf("Functionality not yet supported: %s", "Scan")
}

//IScan ...
func (d *db) IScan(opts *schema.IScanOptions) (*schema.Page, error) {
	//return d.st.IScan(*opts)
	return nil, fmt.Errorf("Functionality not yet supported: %s", "IScan")
}

//Dump ...
func (d *db) Dump(in *empty.Empty, stream schema.ImmuService_DumpServer) error {
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
		err := d.st.Dump(kvChan)
		<-done

		d.Logger.Debugf("Dump stream complete")
		return err
	*/
	return fmt.Errorf("Functionality not yet supported: %s", "Dump")
}

//Close ...
func (d *db) Close() error {
	return d.st.Close()
}

//GetOptions ...
func (d *db) GetOptions() *DbOptions {
	return d.options
}

func logErr(log logger.Logger, formattedMessage string, err error) error {
	if err != nil {
		log.Errorf(formattedMessage, err)
	}
	return err
}

// PrintTree ...
func (d *db) PrintTree() (*schema.Tree, error) {
	return nil, fmt.Errorf("Functionality not yet supported: %s", "PrintTree")
}

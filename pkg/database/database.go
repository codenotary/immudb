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
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/codenotary/immudb/pkg/common"

	"github.com/codenotary/immudb/embedded/store"

	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/codenotary/immudb/pkg/logger"
	"github.com/golang/protobuf/ptypes/empty"
)

type DB interface {
	Health(e *empty.Empty) (*schema.HealthResponse, error)
	CurrentImmutableState() (*schema.ImmutableState, error)
	Set(req *schema.SetRequest) (*schema.TxMetadata, error)
	Get(req *schema.KeyRequest) (*schema.Item, error)
	VerifiableSet(req *schema.VerifiableSetRequest) (*schema.VerifiableTx, error)
	VerifiableGet(req *schema.VerifiableGetRequest) (*schema.VerifiableItem, error)
	GetAll(req *schema.KeyListRequest) (*schema.ItemList, error)
	ExecAllOps(operations *schema.Ops) (*schema.TxMetadata, error)
	Size() (uint64, error)
	Count(prefix *schema.KeyPrefix) (*schema.ItemsCount, error)
	CountAll() *schema.ItemsCount
	TxByID(req *schema.TxRequest) (*schema.Tx, error)
	VerifiableTxByID(req *schema.VerifiableTxRequest) (*schema.VerifiableTx, error)
	History(req *schema.HistoryRequest) (*schema.ItemList, error)
	SetReference(req *schema.Reference) (*schema.TxMetadata, error)
	VerifiableSetReference(req *schema.VerifiableReferenceRequest) (*schema.VerifiableTx, error)
	ZAdd(req *schema.ZAddRequest) (*schema.TxMetadata, error)
	ZScan(req *schema.ZScanRequest) (*schema.ZItemList, error)
	VerifiableZAdd(req *schema.VerifiableZAddRequest) (*schema.VerifiableTx, error)
	Scan(req *schema.ScanRequest) (*schema.ItemList, error)
	IScan(req *schema.IScanRequest) (*schema.Page, error)
	//Dump(in *empty.Empty, stream schema.ImmuService_DumpServer) error
	PrintTree() (*schema.Tree, error)
	Close() error
	GetOptions() *DbOptions
}

//IDB database instance
type db struct {
	st *store.ImmuStore

	tx1, tx2 *store.Tx
	mutex    sync.Mutex

	Logger  logger.Logger
	options *DbOptions
}

// OpenDb Opens an existing Database from disk
func OpenDb(op *DbOptions, log logger.Logger) (DB, error) {
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

	db.st, err = store.Open(dbDir, op.GetStoreOptions())
	if err != nil {
		return nil, logErr(db.Logger, "Unable to open store: %s", err)
	}

	db.tx1 = db.st.NewTx()
	db.tx2 = db.st.NewTx()

	return db, nil
}

// NewDb Creates a new Database along with it's directories and files
func NewDb(op *DbOptions, log logger.Logger) (DB, error) {
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

	db.st, err = store.Open(dbDir, op.GetStoreOptions())
	if err != nil {
		return nil, logErr(db.Logger, "Unable to open store: %s", err)
	}

	db.tx1 = db.st.NewTx()
	db.tx2 = db.st.NewTx()

	return db, nil
}

// Set ...
func (d *db) Set(req *schema.SetRequest) (*schema.TxMetadata, error) {
	if req == nil {
		return nil, store.ErrIllegalArguments
	}

	entries := make([]*store.KV, len(req.KVs))

	for i, kv := range req.KVs {
		entries[i] = &store.KV{Key: kv.Key, Value: kv.Value}
	}

	txMetatadata, err := d.st.Commit(entries)
	if err != nil {
		return nil, err
	}

	return schema.TxMetatadaTo(txMetatadata), nil
}

//Get ...
func (d *db) Get(req *schema.KeyRequest) (*schema.Item, error) {
	err := d.WaitForIndexingUpto(req.FromTx)
	if err != nil {
		return nil, err
	}

	return d.getFrom(req.Key, d.st)
}

func (d *db) WaitForIndexingUpto(txID uint64) error {
	if txID == 0 {
		return nil
	}

	for {
		its, err := d.st.IndexInfo()
		if err != nil {
			return err
		}

		if its >= txID {
			return nil
		}

		time.Sleep(time.Duration(1) * time.Millisecond)
	}
}

type KeyGetter interface {
	Get(key []byte) (value []byte, tx uint64, err error)
}

func (d *db) getFrom(key []byte, keyGetter KeyGetter) (*schema.Item, error) {
	wv, tx, err := keyGetter.Get(key)
	if err != nil {
		return nil, err
	}

	valLen := binary.BigEndian.Uint32(wv)
	vOff := binary.BigEndian.Uint64(wv[4:])

	var hVal [sha256.Size]byte
	copy(hVal[:], wv[4+8:])

	val := make([]byte, valLen)
	_, err = d.st.ReadValueAt(val, int64(vOff), hVal)

	//Reference lookup
	if bytes.HasPrefix(val, common.ReferencePrefix) {
		ref := bytes.TrimPrefix(val, common.ReferencePrefix)
		key, _, _ = common.UnwrapReferenceAt(ref)
		return d.getFrom(key, keyGetter)
	}

	return &schema.Item{Key: key, Value: val, Tx: tx}, err
}

//Health ...
func (d *db) Health(*empty.Empty) (*schema.HealthResponse, error) {
	return &schema.HealthResponse{Status: true, Version: fmt.Sprintf("%d", store.Version)}, nil
}

// CurrentImmutableState ...
func (d *db) CurrentImmutableState() (*schema.ImmutableState, error) {
	lastTxID, lastTxAlh := d.st.Alh()

	return &schema.ImmutableState{
		TxId:   lastTxID,
		TxHash: lastTxAlh[:],
	}, nil
}

//VerifiableSet ...
func (d *db) VerifiableSet(req *schema.VerifiableSetRequest) (*schema.VerifiableTx, error) {
	d.mutex.Lock()
	defer d.mutex.Unlock()

	if req == nil {
		return nil, store.ErrIllegalArguments
	}

	txMetatadata, err := d.Set(req.SetRequest)
	if err != nil {
		return nil, err
	}

	lastTx := d.tx1

	err = d.st.ReadTx(uint64(txMetatadata.Id), lastTx)
	if err != nil {
		return nil, err
	}

	var prevTx *store.Tx

	if req.ProveFromTx == 0 {
		prevTx = lastTx
	} else {
		prevTx = d.tx2

		err = d.st.ReadTx(req.ProveFromTx, prevTx)
		if err != nil {
			return nil, err
		}
	}

	dualProof, err := d.st.DualProof(prevTx, lastTx)
	if err != nil {
		return nil, err
	}

	return &schema.VerifiableTx{
		Tx:        schema.TxTo(lastTx),
		DualProof: schema.DualProofTo(dualProof),
	}, nil
}

//VerifiableGet ...
func (d *db) VerifiableGet(req *schema.VerifiableGetRequest) (*schema.VerifiableItem, error) {
	d.mutex.Lock()
	defer d.mutex.Unlock()

	if req == nil {
		return nil, store.ErrIllegalArguments
	}

	// get value of key
	it, err := d.Get(req.KeyRequest)
	if err != nil {
		return nil, err
	}

	txItem := d.tx1

	// key-value inclusion proof
	err = d.st.ReadTx(it.Tx, txItem)
	if err != nil {
		return nil, err
	}

	inclusionProof, err := d.tx1.Proof(req.KeyRequest.Key)
	if err != nil {
		return nil, err
	}

	var rootTx *store.Tx

	if req.ProveFromTx == 0 {
		rootTx = txItem
	} else {
		rootTx = d.tx2

		err = d.st.ReadTx(req.ProveFromTx, rootTx)
		if err != nil {
			return nil, err
		}
	}

	var sourceTx, targetTx *store.Tx

	if req.ProveFromTx <= it.Tx {
		sourceTx = rootTx
		targetTx = txItem
	} else {
		sourceTx = txItem
		targetTx = rootTx
	}

	dualProof, err := d.st.DualProof(sourceTx, targetTx)
	if err != nil {
		return nil, err
	}

	verifiableTx := &schema.VerifiableTx{
		Tx:        schema.TxTo(txItem),
		DualProof: schema.DualProofTo(dualProof),
	}

	return &schema.VerifiableItem{
		Item:           it,
		VerifiableTx:   verifiableTx,
		InclusionProof: schema.InclusionProofTo(inclusionProof),
	}, nil
}

//GetAll ...
func (d *db) GetAll(req *schema.KeyListRequest) (*schema.ItemList, error) {
	err := d.WaitForIndexingUpto(req.FromTx)
	if err != nil {
		return nil, err
	}

	snapshot, err := d.st.SnapshotAt(req.FromTx)
	if err != nil {
		return nil, err
	}
	defer snapshot.Close()

	list := &schema.ItemList{}

	for _, key := range req.Keys {
		item, err := d.getFrom(key, snapshot)
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

// TxByID ...
func (d *db) TxByID(req *schema.TxRequest) (*schema.Tx, error) {
	d.mutex.Lock()
	defer d.mutex.Unlock()

	if req == nil {
		return nil, store.ErrIllegalArguments
	}

	// key-value inclusion proof
	err := d.st.ReadTx(req.Tx, d.tx1)
	if err != nil {
		return nil, err
	}

	return schema.TxTo(d.tx1), nil
}

//VerifiableTxByID ...
func (d *db) VerifiableTxByID(req *schema.VerifiableTxRequest) (*schema.VerifiableTx, error) {
	d.mutex.Lock()
	defer d.mutex.Unlock()

	if req == nil {
		return nil, store.ErrIllegalArguments
	}

	// key-value inclusion proof
	reqTx := d.tx1

	err := d.st.ReadTx(req.Tx, reqTx)
	if err != nil {
		return nil, err
	}

	var sourceTx, targetTx *store.Tx

	var rootTx *store.Tx

	if req.ProveFromTx == 0 {
		rootTx = reqTx
	} else {
		rootTx = d.tx2

		err = d.st.ReadTx(req.ProveFromTx, rootTx)
		if err != nil {
			return nil, err
		}
	}

	if req.ProveFromTx <= req.Tx {
		sourceTx = rootTx
		targetTx = reqTx
	} else {
		sourceTx = reqTx
		targetTx = rootTx
	}

	dualProof, err := d.st.DualProof(sourceTx, targetTx)
	if err != nil {
		return nil, err
	}

	return &schema.VerifiableTx{
		Tx:        schema.TxTo(reqTx),
		DualProof: schema.DualProofTo(dualProof),
	}, nil
}

//History ...
func (d *db) History(req *schema.HistoryRequest) (*schema.ItemList, error) {
	d.mutex.Lock()
	defer d.mutex.Unlock()

	snapshot, err := d.st.SnapshotAt(req.FromTx)
	if err != nil {
		return nil, err
	}
	defer snapshot.Close()

	limit := math.MaxInt64
	if req.Limit > 0 {
		limit = int(req.Offset + req.Limit)
	}

	tss, err := snapshot.GetTs(req.Key, int64(limit))
	if err != nil {
		return nil, err
	}

	list := &schema.ItemList{}

	for i := int(req.Offset); i < len(tss); i++ {
		ts := tss[i]

		err = d.st.ReadTx(ts, d.tx1)
		if err != nil {
			return nil, err
		}

		val, err := d.st.ReadValue(d.tx1, req.Key)
		if err != nil {
			return nil, err
		}

		item := &schema.Item{Key: req.Key, Value: val, Tx: ts}

		if req.Reverse {
			list.Items = append([]*schema.Item{item}, list.Items...)
		} else {
			list.Items = append(list.Items, item)
		}
	}

	return list, nil
}

//IScan ...
func (d *db) IScan(req *schema.IScanRequest) (*schema.Page, error) {
	//return d.st.IScan(*opts)
	return nil, fmt.Errorf("Functionality not yet supported: %s", "IScan")
}

//Dump ...
/*
func (d *db) Dump(in *empty.Empty, stream schema.ImmuService_DumpServer) error {
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
	return fmt.Errorf("Functionality not yet supported: %s", "Dump")
}
*/

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

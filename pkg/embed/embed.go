package embed

import (
	"context"
	"crypto/sha256"
	"github.com/codenotary/immudb/embedded/store"
	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/codenotary/immudb/pkg/database"
	"github.com/codenotary/immudb/pkg/logger"
	"github.com/codenotary/immudb/pkg/proof"
	"github.com/codenotary/immudb/pkg/state"
	"github.com/codenotary/immudb/pkg/uuid"
	"os"
)

const DefaultDir = "."
const DefaultDatabseName = "mydatabase"

type Embed struct {
	uuid string

	dir          string
	databaseName string

	storeOptions *store.Options
	dbOptions    *database.Options

	log logger.Logger

	db           database.DB
	stateService state.StateService
}

func Open(setters ...EmbedOption) (*Embed, error) {
	e := &Embed{
		dir:          DefaultDir,
		databaseName: DefaultDatabseName,
		storeOptions: store.DefaultOptions(),
		dbOptions:    database.DefaultOption(),
		log:          logger.NewSimpleLogger("embed", os.Stdout),
	}
	for _, setter := range setters {
		setter(e)
	}

	op := e.dbOptions.
		WithDBName(e.databaseName).
		WithDBRootPath(e.dir).
		WithStoreOptions(e.storeOptions)

	xid, err := uuid.GetOrSetUUID(e.dir, e.databaseName)
	if err != nil {
		return nil, err
	}
	e.uuid = xid.String()

	db, err := database.NewDB(op, e.log)
	if err != nil {
		return nil, err
	}
	if e.stateService == nil {
		e.stateService, err = database.NewInMemoryStateService(db)
		if err != nil {
			return nil, err
		}
	}

	if st, ok := e.stateService.(*embedStateService); ok {
		st.InjectDB(db)
		st.InjectUUID(e.uuid)
	}

	e.db = db
	return e, nil
}

func (e *Embed) Set(req *schema.SetRequest) (*schema.TxHeader, error) {
	return e.db.Set(req)
}

func (c *Embed) VerifiedSet(key []byte, value []byte) error {
	// here we retrieve the current state of the database from stateService implementation (CAS)
	state, err := c.stateService.GetState(context.TODO(), c.db.GetName())
	if err != nil {
		return err
	}

	req := &schema.VerifiableSetRequest{
		SetRequest:   &schema.SetRequest{KVs: []*schema.KeyValue{{Key: key, Value: value}}},
		ProveSinceTx: state.TxId,
	}
	// here we write the key value
	verifiableTx, err := c.db.VerifiableSet(req)
	if err != nil {
		return err
	}
	// here verification is done. Basically inclusion is optional. If it is provided immuproof will do the inclusion check, if not only consistency (dual proof)
	err = proof.Verify(verifiableTx, state, &proof.Inclusion{
		Key: key,
		Val: value,
	})
	if err != nil {
		return err
	}

	// here we save the new state on CAS
	tx := schema.TxFromProto(verifiableTx.Tx)
	targetID := tx.Header().ID
	targetAlh := tx.Header().Alh()

	newState := &schema.ImmutableState{
		Db:     c.db.GetName(),
		TxId:   targetID,
		TxHash: targetAlh[:],
	}

	err = c.stateService.SetState(c.db.GetName(), newState)
	if err != nil {
		return err
	}
	return nil
}

// VerifiedGet ...
func (c *Embed) VerifiedGet(key []byte) (vi *schema.Entry, err error) {

	state, err := c.stateService.GetState(context.TODO(), c.db.GetName())
	if err != nil {
		return nil, err
	}

	req := &schema.VerifiableGetRequest{
		KeyRequest: &schema.KeyRequest{
			Key: key,
		},
		ProveSinceTx: state.TxId,
	}

	vEntry, err := c.db.VerifiableGet(req)
	if err != nil {
		return nil, err
	}

	err = proof.Verify(vEntry.VerifiableTx, state, &proof.Inclusion{
		Key: key,
		Val: vEntry.Entry.Value,
	})

	var targetID uint64
	var targetAlh [sha256.Size]byte

	var vTx uint64

	vTx = vEntry.Entry.Tx

	if state.TxId <= vTx {
		targetID = vTx
		targetAlh = schema.DualProofFromProto(vEntry.VerifiableTx.DualProof).TargetTxHeader.Alh()
	} else {
		targetID = state.TxId
		targetAlh = schema.DigestFromProto(state.TxHash)
	}

	newState := &schema.ImmutableState{
		Db:     c.db.GetName(),
		TxId:   targetID,
		TxHash: targetAlh[:],
	}

	err = c.stateService.SetState(c.db.GetName(), newState)
	if err != nil {
		return nil, err
	}

	return vEntry.Entry, nil
}

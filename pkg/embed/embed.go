package embed

import (
	"context"
	"github.com/codenotary/immudb/embedded/store"
	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/codenotary/immudb/pkg/database"
	"github.com/codenotary/immudb/pkg/logger"
	"github.com/codenotary/immudb/pkg/proof"
	"github.com/codenotary/immudb/pkg/state"
	"os"
)

const DefaultDir = "."
const DefaultDatabseName = "mydatabase"

type Embed struct {
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
		Db:        c.db.GetName(),
		TxId:      targetID,
		TxHash:    targetAlh[:],
		Signature: verifiableTx.Signature,
	}

	err = c.stateService.SetState(c.db.GetName(), newState)
	if err != nil {
		return err
	}
	return nil
}

package sql

import "github.com/codenotary/immudb/embedded/store"

type dummyDataSource struct {
	inferParametersFunc func(e *Engine, implicitDB *Database, params map[string]SQLValueType) error
	ResolveFunc         func(e *Engine, snap *store.Snapshot, implicitDB *Database, params map[string]interface{}, ScanSpecs *ScanSpecs) (RowReader, error)
	AliasFunc           func() string
}

func (d *dummyDataSource) inferParameters(e *Engine, implicitDB *Database, params map[string]SQLValueType) error {
	return d.inferParametersFunc(e, implicitDB, params)
}

func (d *dummyDataSource) Resolve(e *Engine, snap *store.Snapshot, implicitDB *Database, params map[string]interface{}, scanSpecs *ScanSpecs) (RowReader, error) {
	return d.ResolveFunc(e, snap, implicitDB, params, scanSpecs)
}

func (d *dummyDataSource) Alias() string {
	return d.AliasFunc()
}

package sql

type dummyDataSource struct {
	inferParametersFunc func(tx *SQLTx, implicitDB *Database, params map[string]SQLValueType) error
	ResolveFunc         func(tx *SQLTx, implicitDB *Database, params map[string]interface{}, ScanSpecs *ScanSpecs) (RowReader, error)
	AliasFunc           func() string
}

func (d *dummyDataSource) inferParameters(tx *SQLTx, implicitDB *Database, params map[string]SQLValueType) error {
	return d.inferParametersFunc(tx, implicitDB, params)
}

func (d *dummyDataSource) Resolve(tx *SQLTx, implicitDB *Database, params map[string]interface{}, scanSpecs *ScanSpecs) (RowReader, error) {
	return d.ResolveFunc(tx, implicitDB, params, scanSpecs)
}

func (d *dummyDataSource) Alias() string {
	return d.AliasFunc()
}

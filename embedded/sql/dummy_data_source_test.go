package sql

type dummyDataSource struct {
	inferParametersFunc func(tx *SQLTx, params map[string]SQLValueType) error
	ResolveFunc         func(tx *SQLTx, params map[string]interface{}, ScanSpecs *ScanSpecs) (RowReader, error)
	AliasFunc           func() string
}

func (d *dummyDataSource) execAt(tx *SQLTx, params map[string]interface{}) (*SQLTx, error) {
	return tx, nil
}

func (d *dummyDataSource) inferParameters(tx *SQLTx, params map[string]SQLValueType) error {
	return d.inferParametersFunc(tx, params)
}

func (d *dummyDataSource) Resolve(tx *SQLTx, params map[string]interface{}, scanSpecs *ScanSpecs) (RowReader, error) {
	return d.ResolveFunc(tx, params, scanSpecs)
}

func (d *dummyDataSource) Alias() string {
	return d.AliasFunc()
}

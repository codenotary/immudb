package sql

import "context"

type dummyDataSource struct {
	inferParametersFunc func(ctx context.Context, tx *SQLTx, params map[string]SQLValueType) error
	ResolveFunc         func(ctx context.Context, tx *SQLTx, params map[string]interface{}, ScanSpecs *ScanSpecs) (RowReader, error)
	AliasFunc           func() string
}

func (d *dummyDataSource) execAt(ctx context.Context, tx *SQLTx, params map[string]interface{}) (*SQLTx, error) {
	return tx, nil
}

func (d *dummyDataSource) inferParameters(ctx context.Context, tx *SQLTx, params map[string]SQLValueType) error {
	return d.inferParametersFunc(ctx, tx, params)
}

func (d *dummyDataSource) Resolve(ctx context.Context, tx *SQLTx, params map[string]interface{}, scanSpecs *ScanSpecs) (RowReader, error) {
	return d.ResolveFunc(ctx, tx, params, scanSpecs)
}

func (d *dummyDataSource) Alias() string {
	return d.AliasFunc()
}

package schema

// LastInsertedPk returns a map in which the keys are the names of the tables and the values are the primary keys of the last inserted rows.
func (er *SQLExecResult) LastInsertedPk() map[string]*SQLValue {
	if len(er.Txs) > 0 {
		return er.Txs[len(er.Txs)-1].LastInsertedPKs
	}
	return nil
}

// FirstInsertedPks returns a map in which the keys are the names of the tables and the values are the primary keys of the first inserted rows.
func (er *SQLExecResult) FirstInsertedPks() map[string]*SQLValue {
	if len(er.Txs) > 0 {
		return er.Txs[0].FirstInsertedPKs
	}
	return nil
}

package stdlib

import (
	"context"
	"database/sql/driver"
)

type dbTx struct {
	*Conn
}

func (c *Conn) Begin() (driver.Tx, error) {
	return c.BeginTx(context.TODO(), driver.TxOptions{})
}

func (c *Conn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	if !c.immuClient.IsConnected() {
		return nil, driver.ErrBadConn
	}

	tx, err := c.immuClient.NewTx(ctx)
	if err != nil {
		return nil, err
	}

	c.tx = tx

	return &dbTx{c}, nil
}

func (dbTx *dbTx) Commit() error {
	_, err := dbTx.tx.Commit(context.TODO())
	dbTx.tx = nil
	return err
}

func (dbTx *dbTx) Rollback() error {
	err := dbTx.tx.Rollback(context.TODO())
	dbTx.tx = nil
	return err
}

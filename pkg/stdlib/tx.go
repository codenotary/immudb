package stdlib

import (
	"context"
	"database/sql/driver"
	"github.com/codenotary/immudb/pkg/client"
)

type dbTx struct {
	immuTx client.Tx
	c      *Conn
}

func (c *Conn) Begin() (driver.Tx, error) {
	return c.BeginTx(context.TODO(), driver.TxOptions{})
}

func (c *Conn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	if !c.conn.IsConnected() {
		return nil, driver.ErrBadConn
	}
	tx, err := c.conn.NewTx(ctx)
	if err != nil {
		return nil, err
	}
	c.tx = tx
	return &dbTx{immuTx: tx, c: c}, nil
}

func (dbTx *dbTx) Commit() error {
	_, err := dbTx.immuTx.Commit(context.TODO())
	dbTx.c.tx = nil
	return err
}

func (dbTx *dbTx) Rollback() error {
	return dbTx.immuTx.Rollback(context.TODO())
}

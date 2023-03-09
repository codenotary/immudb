package document

import (
	"github.com/codenotary/immudb/embedded/sql"
)

var ObjectPrefix = []byte{3}

func DefaultOptions() *sql.Options {
	return sql.DefaultOptions().
		WithPrefix(ObjectPrefix)
}

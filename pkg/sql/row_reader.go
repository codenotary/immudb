/*
Copyright 2021 CodeNotary, Inc. All rights reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

	http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package sql

import (
	"encoding/binary"

	"github.com/codenotary/immudb/embedded/store"
	"github.com/codenotary/immudb/embedded/tbtree"
)

type RowReader interface {
	Read() (*Row, error)
	Close() error
}

type Row struct {
	Values map[string]Value
}

type rawRowReader struct {
	e      *Engine
	snap   *tbtree.Snapshot
	table  *Table
	col    string
	desc   bool
	reader *store.KeyReader
}

func (e *Engine) newRawRowReader(snap *tbtree.Snapshot, table *Table, colName string, cmp Comparison, initKeyVal []byte) (*rawRowReader, error) {
	if snap == nil || table == nil {
		return nil, ErrIllegalArguments
	}

	col, exist := table.colsByName[colName]
	if !exist {
		return nil, ErrColumnDoesNotExist
	}

	prefix := e.mapKey(rowPrefix, encodeID(table.db.id), encodeID(table.id), encodeID(col.id))

	if cmp == EqualTo {
		prefix = append(prefix, initKeyVal...)
	}

	skey := make([]byte, len(prefix))
	copy(skey, prefix)

	if cmp == GreaterThan || cmp == GreaterOrEqualTo {
		skey = append(skey, initKeyVal...)
	}

	if cmp == LowerThan || cmp == LowerOrEqualTo {
		if table.pk.colName == colName {
			skey = append(skey, initKeyVal...)
		} else {
			maxPKVal := maxKeyVal(table.pk.colType)

			skey = append(skey, initKeyVal...)
			skey = append(skey, maxPKVal...)
		}
	}

	rSpec := &tbtree.ReaderSpec{
		SeekKey:       skey,
		InclusiveSeek: cmp != LowerThan && cmp != GreaterThan,
		Prefix:        prefix,
		DescOrder:     cmp == LowerThan || cmp == LowerOrEqualTo,
	}

	r, err := e.dataStore.NewKeyReader(snap, rSpec)
	if err != nil {
		return nil, err
	}

	return &rawRowReader{
		e:      e,
		snap:   snap,
		table:  table,
		col:    col.colName,
		desc:   rSpec.DescOrder,
		reader: r,
	}, nil
}

func (r *rawRowReader) Read() (*Row, error) {
	mkey, vref, _, _, err := r.reader.Read()
	if err != nil {
		return nil, err
	}

	var v []byte

	//decompose key, determine if it's pk, when it's pk, the value holds the actual row data
	if r.table.pk.colName == r.col {
		v, err = vref.Resolve()
		if err != nil {
			return nil, err
		}
	} else {
		_, _, _, _, pkVal, err := r.e.unmapRow(mkey)
		if err != nil {
			return nil, err
		}

		v, _, _, err = r.snap.Get(r.e.mapKey(rowPrefix, encodeID(r.table.db.id), encodeID(r.table.id), encodeID(r.table.pk.id), pkVal))
	}

	values := make(map[string]Value, len(r.table.colsByID))

	voff := 0

	cols := int(binary.BigEndian.Uint32(v[voff:]))
	voff += 4

	for i := 0; i < cols; i++ {
		cNameLen := int(binary.BigEndian.Uint32(v[voff:]))
		voff += 4
		colName := string(v[voff : voff+cNameLen])
		voff += cNameLen

		col, ok := r.table.colsByName[colName]
		if !ok {
			return nil, ErrInvalidColumn
		}

		switch col.colType {
		case StringType:
			{
				vlen := int(binary.BigEndian.Uint32(v[voff:]))
				voff += 4

				v := string(v[voff : voff+vlen])
				voff += vlen
				values[r.table.db.name+"."+r.table.name+"."+colName] = &String{val: v}
			}
		case IntegerType:
			{
				v := binary.BigEndian.Uint64(v[voff:])
				voff += 8
				values[r.table.db.name+"."+r.table.name+"."+colName] = &Number{val: v}
			}
		}
	}

	return &Row{Values: values}, nil
}

func (r *rawRowReader) Close() error {
	return r.reader.Close()
}

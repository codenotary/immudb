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
	ImplicitDB() string
	Alias() string
	Read() (*Row, error)
	Close() error
	Columns() []*ColDescriptor
}

type ColDescriptor struct {
	ColName string
	ColType SQLValueType
}

type Row struct {
	ImplicitDB   string
	ImplictTable string
	Values       map[string]TypedValue
}

// rows are selector-compatible if both rows have the same assigned value for all specified selectors
func (row *Row) Compatible(aRow *Row, selectors []*ColSelector, db, table string) (bool, error) {
	for _, sel := range selectors {
		c := EncodeSelector(sel.resolve(db, table))

		val1, ok := row.Values[c]
		if !ok {
			return false, ErrInvalidColumn
		}

		val2, ok := aRow.Values[c]
		if !ok {
			return false, ErrInvalidColumn
		}

		cmp, err := val1.Compare(val2)
		if err != nil {
			return false, err
		}

		if cmp != 0 {
			return false, nil
		}
	}

	return true, nil
}

type rawRowReader struct {
	e              *Engine
	implicitDB     string
	snap           *store.Snapshot
	table          *Table
	colDescriptors []*ColDescriptor
	alias          string
	col            string
	desc           bool
	reader         *store.KeyReader
}

func (e *Engine) newRawRowReader(snap *store.Snapshot, table *Table, alias string, colName string, cmp Comparison, encInitKeyVal []byte) (*rawRowReader, error) {
	if snap == nil || table == nil {
		return nil, ErrIllegalArguments
	}

	col, exist := table.colsByName[colName]
	if !exist {
		return nil, ErrColumnDoesNotExist
	}

	prefix := e.mapKey(rowPrefix, encodeID(table.db.id), encodeID(table.id), encodeID(col.id))

	if cmp == EqualTo {
		prefix = append(prefix, encInitKeyVal...)
	}

	skey := make([]byte, len(prefix))
	copy(skey, prefix)

	if cmp == GreaterThan || cmp == GreaterOrEqualTo {
		skey = append(skey, encInitKeyVal...)
	}

	if cmp == LowerThan || cmp == LowerOrEqualTo {
		if table.pk.colName == colName {
			skey = append(skey, encInitKeyVal...)
		} else {
			maxPKVal := maxKeyVal(table.pk.colType)

			skey = append(skey, encInitKeyVal...)
			skey = append(skey, maxPKVal...)
		}
	}

	rSpec := &tbtree.ReaderSpec{
		SeekKey:       skey,
		InclusiveSeek: cmp != LowerThan && cmp != GreaterThan,
		Prefix:        prefix,
		DescOrder:     cmp == LowerThan || cmp == LowerOrEqualTo,
	}

	r, err := snap.NewKeyReader(rSpec)
	if err != nil {
		return nil, err
	}

	if alias == "" {
		alias = table.name
	}

	colDescriptors := make([]*ColDescriptor, len(table.colsByID))

	i := 0
	for _, c := range table.colsByID {
		colDescriptors[i] = &ColDescriptor{ColName: c.colName, ColType: c.colType}
		i++
	}

	return &rawRowReader{
		e:              e,
		implicitDB:     e.ImplicitDB(),
		snap:           snap,
		table:          table,
		colDescriptors: colDescriptors,
		alias:          alias,
		col:            col.colName,
		desc:           rSpec.DescOrder,
		reader:         r,
	}, nil
}

func (r *rawRowReader) ImplicitDB() string {
	return r.implicitDB
}

func (r *rawRowReader) Columns() []*ColDescriptor {
	return r.colDescriptors
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
		_, _, _, _, encPKVal, err := r.e.unmapIndexedRow(mkey)
		if err != nil {
			return nil, err
		}

		v, _, _, err = r.snap.Get(r.e.mapKey(rowPrefix, encodeID(r.table.db.id), encodeID(r.table.id), encodeID(r.table.pk.id), encPKVal))
		if err != nil {
			return nil, err
		}
	}

	values := make(map[string]TypedValue, len(r.table.colsByID))

	voff := 0

	cols := int(binary.BigEndian.Uint32(v[voff:]))
	voff += encLenLen

	for i := 0; i < cols; i++ {
		if len(v) < encLenLen {
			return nil, ErrCorruptedData
		}
		cNameLen := int(binary.BigEndian.Uint32(v[voff:]))
		voff += encLenLen

		if len(v) < cNameLen {
			return nil, ErrCorruptedData
		}
		colName := string(v[voff : voff+cNameLen])
		voff += cNameLen

		col, ok := r.table.colsByName[colName]
		if !ok {
			return nil, ErrCorruptedData
		}

		val, n, err := DecodeValue(v[voff:], col.colType)
		if err != nil {
			return nil, err
		}

		voff += n
		values[EncodeSelector("", r.table.db.name, r.alias, colName)] = val
	}

	return &Row{ImplicitDB: r.e.implicitDB, ImplictTable: r.alias, Values: values}, nil
}

func (r *rawRowReader) Alias() string {
	return r.alias
}

func (r *rawRowReader) Close() error {
	defer r.snap.Close()

	return r.reader.Close()
}

/*
Copyright 2025 Codenotary Inc. All rights reserved.

SPDX-License-Identifier: BUSL-1.1
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    https://mariadb.com/bsl11/

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package sql

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/codenotary/immudb/embedded/store"
	"github.com/google/uuid"
)

const (
	catalogPrefix          = "CTL."
	catalogTablePrefix     = "CTL.TABLE."     // (key=CTL.TABLE.{1}{tableID}, value={tableNAME})
	catalogColumnPrefix    = "CTL.COLUMN."    // (key=CTL.COLUMN.{1}{tableID}{colID}{colTYPE}, value={(auto_incremental | nullable){maxLen}{colNAME}})
	catalogIndexPrefix     = "CTL.INDEX."     // (key=CTL.INDEX.{1}{tableID}{indexID}, value={unique {colID1}(ASC|DESC)...{colIDN}(ASC|DESC)})
	catalogCheckPrefix     = "CTL.CHECK."     // (key=CTL.CHECK.{1}{tableID}{checkID}, value={nameLen}{name}{expText})
	catalogPrivilegePrefix = "CTL.PRIVILEGE." // (key=CTL.COLUMN.{1}{tableID}{colID}{colTYPE}, value={(auto_incremental | nullable){maxLen}{colNAME}})
	catalogViewPrefix      = "CTL.VIEW."      // (key=CTL.VIEW.{1}{viewID}, value={viewName\0sqlText})
	catalogSequencePrefix  = "CTL.SEQUENCE."  // (key=CTL.SEQUENCE.{1}{seqName}, value={currValue}{increment})

	RowPrefix    = "R." // (key=R.{1}{tableID}{0}({null}({pkVal}{padding}{pkValLen})?)+, value={count (colID valLen val)+})
	MappedPrefix = "M." // (key=M.{tableID}{indexID}({null}({val}{padding}{valLen})?)*({pkVal}{padding}{pkValLen})+, value={count (colID valLen val)+})
)

const (
	DatabaseID = uint32(1) // deprecated but left to maintain backwards compatibility
	PKIndexID  = uint32(0)
)

const (
	nullableFlag      byte = 1 << iota
	autoIncrementFlag byte = 1 << iota
	hasDefaultFlag    byte = 1 << iota
)

const (
	revCol        = "_rev"
	txMetadataCol = "_tx_metadata"
	diffActionCol = "_diff_action"
)

var reservedColumns = map[string]struct{}{
	revCol:        {},
	txMetadataCol: {},
	diffActionCol: {},
}

func isReservedCol(col string) bool {
	_, ok := reservedColumns[col]
	return ok
}

type SQLValueType = string

const (
	IntegerType   SQLValueType = "INTEGER"
	BooleanType   SQLValueType = "BOOLEAN"
	VarcharType   SQLValueType = "VARCHAR"
	UUIDType      SQLValueType = "UUID"
	BLOBType      SQLValueType = "BLOB"
	Float64Type   SQLValueType = "FLOAT"
	TimestampType SQLValueType = "TIMESTAMP"
	AnyType       SQLValueType = "ANY"
	JSONType      SQLValueType = "JSON"
)

func IsNumericType(t SQLValueType) bool {
	return t == IntegerType || t == Float64Type
}

type Permission = string

const (
	PermissionReadOnly  Permission = "READ"
	PermissionReadWrite Permission = "READWRITE"
	PermissionAdmin     Permission = "ADMIN"
	PermissionSysAdmin  Permission = "SYSADMIN"
)

func PermissionFromCode(code uint32) Permission {
	switch code {
	case 1:
		{
			return PermissionReadOnly
		}
	case 2:
		{
			return PermissionReadWrite
		}
	case 254:
		{
			return PermissionAdmin
		}
	}
	return PermissionSysAdmin
}

type AggregateFn = string

const (
	COUNT      AggregateFn = "COUNT"
	SUM        AggregateFn = "SUM"
	MAX        AggregateFn = "MAX"
	MIN        AggregateFn = "MIN"
	AVG        AggregateFn = "AVG"
	STRING_AGG AggregateFn = "STRING_AGG"
)

type CmpOperator = int

const (
	EQ CmpOperator = iota
	NE
	LT
	LE
	GT
	GE
)

func CmpOperatorToString(op CmpOperator) string {
	switch op {
	case EQ:
		return "="
	case NE:
		return "!="
	case LT:
		return "<"
	case LE:
		return "<="
	case GT:
		return ">"
	case GE:
		return ">="
	}
	return ""
}

type LogicOperator = int

const (
	And LogicOperator = iota
	Or
)

func LogicOperatorToString(op LogicOperator) string {
	if op == And {
		return "AND"
	}
	return "OR"
}

type NumOperator = int

const (
	ADDOP NumOperator = iota
	SUBSOP
	DIVOP
	MULTOP
	MODOP
)

func NumOperatorString(op NumOperator) string {
	switch op {
	case ADDOP:
		return "+"
	case SUBSOP:
		return "-"
	case DIVOP:
		return "/"
	case MULTOP:
		return "*"
	case MODOP:
		return "%"
	}
	return ""
}

type JoinType = int

const (
	InnerJoin JoinType = iota
	LeftJoin
	RightJoin
	CrossJoin
	FullOuterJoin
)

type SQLStmt interface {
	readOnly() bool
	requiredPrivileges() []SQLPrivilege
	execAt(ctx context.Context, tx *SQLTx, params map[string]interface{}) (*SQLTx, error)
	inferParameters(ctx context.Context, tx *SQLTx, params map[string]SQLValueType) error
}

type BeginTransactionStmt struct {
}

func (stmt *BeginTransactionStmt) readOnly() bool {
	return true
}

func (stmt *BeginTransactionStmt) requiredPrivileges() []SQLPrivilege {
	return nil
}

func (stmt *BeginTransactionStmt) inferParameters(ctx context.Context, tx *SQLTx, params map[string]SQLValueType) error {
	return nil
}

func (stmt *BeginTransactionStmt) execAt(ctx context.Context, tx *SQLTx, params map[string]interface{}) (*SQLTx, error) {
	if tx.IsExplicitCloseRequired() {
		return nil, ErrNestedTxNotSupported
	}

	err := tx.RequireExplicitClose()
	if err == nil {
		// current tx can be reused as no changes were already made
		return tx, nil
	}

	// commit current transaction and start a fresh one

	err = tx.Commit(ctx)
	if err != nil {
		return nil, err
	}

	return tx.engine.NewTx(ctx, tx.opts.WithExplicitClose(true))
}

type CommitStmt struct {
}

func (stmt *CommitStmt) readOnly() bool {
	return true
}

func (stmt *CommitStmt) requiredPrivileges() []SQLPrivilege {
	return nil
}

func (stmt *CommitStmt) inferParameters(ctx context.Context, tx *SQLTx, params map[string]SQLValueType) error {
	return nil
}

func (stmt *CommitStmt) execAt(ctx context.Context, tx *SQLTx, params map[string]interface{}) (*SQLTx, error) {
	if !tx.IsExplicitCloseRequired() {
		return nil, ErrNoOngoingTx
	}

	return nil, tx.Commit(ctx)
}

type RollbackStmt struct {
}

func (stmt *RollbackStmt) readOnly() bool {
	return true
}

func (stmt *RollbackStmt) requiredPrivileges() []SQLPrivilege {
	return nil
}

func (stmt *RollbackStmt) inferParameters(ctx context.Context, tx *SQLTx, params map[string]SQLValueType) error {
	return nil
}

func (stmt *RollbackStmt) execAt(ctx context.Context, tx *SQLTx, params map[string]interface{}) (*SQLTx, error) {
	if !tx.IsExplicitCloseRequired() {
		return nil, ErrNoOngoingTx
	}

	return nil, tx.Cancel()
}

type SavepointStmt struct {
	name string
}

func (stmt *SavepointStmt) readOnly() bool                     { return true }
func (stmt *SavepointStmt) requiredPrivileges() []SQLPrivilege { return nil }
func (stmt *SavepointStmt) inferParameters(ctx context.Context, tx *SQLTx, params map[string]SQLValueType) error {
	return nil
}

func (stmt *SavepointStmt) execAt(ctx context.Context, tx *SQLTx, params map[string]interface{}) (*SQLTx, error) {
	if !tx.IsExplicitCloseRequired() {
		return nil, ErrNoOngoingTx
	}
	tx.Savepoint(stmt.name)
	return tx, nil
}

type ReleaseSavepointStmt struct {
	name string
}

func (stmt *ReleaseSavepointStmt) readOnly() bool                     { return true }
func (stmt *ReleaseSavepointStmt) requiredPrivileges() []SQLPrivilege { return nil }
func (stmt *ReleaseSavepointStmt) inferParameters(ctx context.Context, tx *SQLTx, params map[string]SQLValueType) error {
	return nil
}

func (stmt *ReleaseSavepointStmt) execAt(ctx context.Context, tx *SQLTx, params map[string]interface{}) (*SQLTx, error) {
	if !tx.IsExplicitCloseRequired() {
		return nil, ErrNoOngoingTx
	}
	return tx, tx.ReleaseSavepoint(stmt.name)
}

type RollbackToSavepointStmt struct {
	name string
}

func (stmt *RollbackToSavepointStmt) readOnly() bool                     { return true }
func (stmt *RollbackToSavepointStmt) requiredPrivileges() []SQLPrivilege { return nil }
func (stmt *RollbackToSavepointStmt) inferParameters(ctx context.Context, tx *SQLTx, params map[string]SQLValueType) error {
	return nil
}

func (stmt *RollbackToSavepointStmt) execAt(ctx context.Context, tx *SQLTx, params map[string]interface{}) (*SQLTx, error) {
	if !tx.IsExplicitCloseRequired() {
		return nil, ErrNoOngoingTx
	}
	return tx, tx.RollbackToSavepoint(stmt.name)
}

type CreateDatabaseStmt struct {
	DB          string
	ifNotExists bool
}

func (stmt *CreateDatabaseStmt) readOnly() bool {
	return false
}

func (stmt *CreateDatabaseStmt) requiredPrivileges() []SQLPrivilege {
	return []SQLPrivilege{SQLPrivilegeCreate}
}

func (stmt *CreateDatabaseStmt) inferParameters(ctx context.Context, tx *SQLTx, params map[string]SQLValueType) error {
	return nil
}

func (stmt *CreateDatabaseStmt) execAt(ctx context.Context, tx *SQLTx, params map[string]interface{}) (*SQLTx, error) {
	if tx.IsExplicitCloseRequired() {
		return nil, fmt.Errorf("%w: database creation can not be done within a transaction", ErrNonTransactionalStmt)
	}

	if tx.engine.multidbHandler == nil {
		return nil, ErrUnspecifiedMultiDBHandler
	}

	return nil, tx.engine.multidbHandler.CreateDatabase(ctx, stmt.DB, stmt.ifNotExists)
}

type UseDatabaseStmt struct {
	DB string
}

func (stmt *UseDatabaseStmt) inferParameters(ctx context.Context, tx *SQLTx, params map[string]SQLValueType) error {
	return nil
}

func (stmt *UseDatabaseStmt) readOnly() bool {
	return true
}

func (stmt *UseDatabaseStmt) requiredPrivileges() []SQLPrivilege {
	return nil
}

func (stmt *UseDatabaseStmt) execAt(ctx context.Context, tx *SQLTx, params map[string]interface{}) (*SQLTx, error) {
	if tx.IsExplicitCloseRequired() {
		return nil, fmt.Errorf("%w: database selection can NOT be executed within a transaction block", ErrNonTransactionalStmt)
	}

	if tx.engine.multidbHandler == nil {
		return nil, ErrUnspecifiedMultiDBHandler
	}

	return tx, tx.engine.multidbHandler.UseDatabase(ctx, stmt.DB)
}

type UseSnapshotStmt struct {
	period period
}

func (stmt *UseSnapshotStmt) readOnly() bool {
	return true
}

func (stmt *UseSnapshotStmt) requiredPrivileges() []SQLPrivilege {
	return nil
}

func (stmt *UseSnapshotStmt) inferParameters(ctx context.Context, tx *SQLTx, params map[string]SQLValueType) error {
	return nil
}

func (stmt *UseSnapshotStmt) execAt(ctx context.Context, tx *SQLTx, params map[string]interface{}) (*SQLTx, error) {
	return nil, ErrNoSupported
}

type CreateUserStmt struct {
	username   string
	password   string
	permission Permission
}

func (stmt *CreateUserStmt) inferParameters(ctx context.Context, tx *SQLTx, params map[string]SQLValueType) error {
	return nil
}

func (stmt *CreateUserStmt) readOnly() bool {
	return false
}

func (stmt *CreateUserStmt) requiredPrivileges() []SQLPrivilege {
	return []SQLPrivilege{SQLPrivilegeCreate}
}

func (stmt *CreateUserStmt) execAt(ctx context.Context, tx *SQLTx, params map[string]interface{}) (*SQLTx, error) {
	if tx.IsExplicitCloseRequired() {
		return nil, fmt.Errorf("%w: user creation can not be done within a transaction", ErrNonTransactionalStmt)
	}

	if tx.engine.multidbHandler == nil {
		return nil, ErrUnspecifiedMultiDBHandler
	}

	return nil, tx.engine.multidbHandler.CreateUser(ctx, stmt.username, stmt.password, stmt.permission)
}

type AlterUserStmt struct {
	username   string
	password   string
	permission Permission
}

func (stmt *AlterUserStmt) readOnly() bool {
	return false
}

func (stmt *AlterUserStmt) requiredPrivileges() []SQLPrivilege {
	return []SQLPrivilege{SQLPrivilegeAlter}
}

func (stmt *AlterUserStmt) inferParameters(ctx context.Context, tx *SQLTx, params map[string]SQLValueType) error {
	return nil
}

func (stmt *AlterUserStmt) execAt(ctx context.Context, tx *SQLTx, params map[string]interface{}) (*SQLTx, error) {
	if tx.IsExplicitCloseRequired() {
		return nil, fmt.Errorf("%w: user modification can not be done within a transaction", ErrNonTransactionalStmt)
	}

	if tx.engine.multidbHandler == nil {
		return nil, ErrUnspecifiedMultiDBHandler
	}

	return nil, tx.engine.multidbHandler.AlterUser(ctx, stmt.username, stmt.password, stmt.permission)
}

type DropUserStmt struct {
	username string
}

func (stmt *DropUserStmt) readOnly() bool {
	return false
}

func (stmt *DropUserStmt) requiredPrivileges() []SQLPrivilege {
	return []SQLPrivilege{SQLPrivilegeDrop}
}

func (stmt *DropUserStmt) inferParameters(ctx context.Context, tx *SQLTx, params map[string]SQLValueType) error {
	return nil
}

func (stmt *DropUserStmt) execAt(ctx context.Context, tx *SQLTx, params map[string]interface{}) (*SQLTx, error) {
	if tx.IsExplicitCloseRequired() {
		return nil, fmt.Errorf("%w: user deletion can not be done within a transaction", ErrNonTransactionalStmt)
	}

	if tx.engine.multidbHandler == nil {
		return nil, ErrUnspecifiedMultiDBHandler
	}

	return nil, tx.engine.multidbHandler.DropUser(ctx, stmt.username)
}

type TableElem interface{}

type CreateTableStmt struct {
	table       string
	ifNotExists bool
	colsSpec    []*ColSpec
	checks      []CheckConstraint
	pkColNames  PrimaryKeyConstraint
}

func NewCreateTableStmt(table string, ifNotExists bool, colsSpec []*ColSpec, pkColNames []string) *CreateTableStmt {
	return &CreateTableStmt{table: table, ifNotExists: ifNotExists, colsSpec: colsSpec, pkColNames: pkColNames}
}

func (stmt *CreateTableStmt) readOnly() bool {
	return false
}

func (stmt *CreateTableStmt) requiredPrivileges() []SQLPrivilege {
	return []SQLPrivilege{SQLPrivilegeCreate}
}

func (stmt *CreateTableStmt) inferParameters(ctx context.Context, tx *SQLTx, params map[string]SQLValueType) error {
	return nil
}

func zeroRow(tableName string, cols []*ColSpec) *Row {
	r := Row{
		ValuesByPosition: make([]TypedValue, len(cols)),
		ValuesBySelector: make(map[string]TypedValue, len(cols)),
	}

	for i, col := range cols {
		v := zeroForType(col.colType)

		r.ValuesByPosition[i] = v
		r.ValuesBySelector[EncodeSelector("", tableName, col.colName)] = v
	}
	return &r
}

func (stmt *CreateTableStmt) execAt(ctx context.Context, tx *SQLTx, params map[string]interface{}) (*SQLTx, error) {
	if err := stmt.validatePrimaryKey(); err != nil {
		return nil, err
	}

	if stmt.ifNotExists && tx.catalog.ExistTable(stmt.table) {
		return tx, nil
	}

	colSpecs := make(map[uint32]*ColSpec, len(stmt.colsSpec))
	for i, cs := range stmt.colsSpec {
		colSpecs[uint32(i)+1] = cs
	}

	row := zeroRow(stmt.table, stmt.colsSpec)
	for _, check := range stmt.checks {
		value, err := check.exp.reduce(tx, row, stmt.table)
		if err != nil {
			return nil, err
		}

		if value.Type() != BooleanType {
			return nil, ErrInvalidCheckConstraint
		}
	}

	nextUnnamedCheck := 0
	checks := make(map[string]CheckConstraint)
	for id, check := range stmt.checks {
		name := fmt.Sprintf("%s_check%d", stmt.table, nextUnnamedCheck+1)
		if check.name != "" {
			name = check.name
		} else {
			nextUnnamedCheck++
		}
		check.id = uint32(id)
		check.name = name
		checks[name] = check
	}

	table, err := tx.catalog.newTable(stmt.table, colSpecs, checks, uint32(len(colSpecs)))
	if err != nil {
		return nil, err
	}

	createIndexStmt := &CreateIndexStmt{unique: true, table: table.name, cols: stmt.primaryKeyCols()}
	_, err = createIndexStmt.execAt(ctx, tx, params)
	if err != nil {
		return nil, err
	}

	for _, col := range table.cols {
		if col.autoIncrement {
			if len(table.primaryIndex.cols) > 1 || col.id != table.primaryIndex.cols[0].id {
				return nil, ErrLimitedAutoIncrement
			}
		}

		err := persistColumn(tx, col)
		if err != nil {
			return nil, err
		}
	}

	for _, check := range checks {
		if err := persistCheck(tx, table, &check); err != nil {
			return nil, err
		}
	}

	mappedKey := MapKey(tx.sqlPrefix(), catalogTablePrefix, EncodeID(DatabaseID), EncodeID(table.id))

	err = tx.set(mappedKey, nil, []byte(table.name))
	if err != nil {
		return nil, err
	}

	tx.mutatedCatalog = true

	return tx, nil
}

func (stmt *CreateTableStmt) validatePrimaryKey() error {
	n := 0
	for _, spec := range stmt.colsSpec {
		if spec.primaryKey {
			n++
		}
	}

	if len(stmt.pkColNames) > 0 {
		n++
	}

	switch n {
	case 0:
		return ErrNoPrimaryKey
	case 1:
		return nil
	}
	return fmt.Errorf("\"%s\": %w", stmt.table, ErrMultiplePrimaryKeys)
}

func (stmt *CreateTableStmt) primaryKeyCols() []string {
	if len(stmt.pkColNames) > 0 {
		return stmt.pkColNames
	}

	for _, spec := range stmt.colsSpec {
		if spec.primaryKey {
			return []string{spec.colName}
		}
	}
	return nil
}

func persistColumn(tx *SQLTx, col *Column) error {
	var defaultSQL string
	hasDefault := col.defaultValue != nil
	if hasDefault {
		defaultSQL = col.defaultValue.String()
	}

	colNameBytes := []byte(col.Name())

	var v []byte
	if hasDefault {
		// New format: {flags(1)}{maxLen(4)}{colNameLen(2)}{colName}{defaultSQL}
		v = make([]byte, 1+4+2+len(colNameBytes)+len(defaultSQL))
		binary.BigEndian.PutUint16(v[5:], uint16(len(colNameBytes)))
		copy(v[7:], colNameBytes)
		copy(v[7+len(colNameBytes):], []byte(defaultSQL))
	} else {
		// Old format: {flags(1)}{maxLen(4)}{colName}
		v = make([]byte, 1+4+len(colNameBytes))
		copy(v[5:], colNameBytes)
	}

	if col.autoIncrement {
		v[0] = v[0] | autoIncrementFlag
	}

	if col.notNull {
		v[0] = v[0] | nullableFlag
	}

	if hasDefault {
		v[0] = v[0] | hasDefaultFlag
	}

	binary.BigEndian.PutUint32(v[1:], uint32(col.MaxLen()))

	mappedKey := MapKey(
		tx.sqlPrefix(),
		catalogColumnPrefix,
		EncodeID(DatabaseID),
		EncodeID(col.table.id),
		EncodeID(col.id),
		[]byte(col.colType),
	)

	return tx.set(mappedKey, nil, v)
}

func persistCheck(tx *SQLTx, table *Table, check *CheckConstraint) error {
	mappedKey := MapKey(
		tx.sqlPrefix(),
		catalogCheckPrefix,
		EncodeID(DatabaseID),
		EncodeID(table.id),
		EncodeID(check.id),
	)

	name := check.name
	expText := check.exp.String()

	val := make([]byte, 2+len(name)+len(expText))

	if len(name) > 256 {
		return fmt.Errorf("constraint name len: %w", ErrMaxLengthExceeded)
	}

	val[0] = byte(len(name)) - 1

	copy(val[1:], []byte(name))
	copy(val[1+len(name):], []byte(expText))

	return tx.set(mappedKey, nil, val)
}

func persistView(tx *SQLTx, viewName string, sqlText string) error {
	mappedKey := MapKey(
		tx.sqlPrefix(),
		catalogViewPrefix,
		EncodeID(DatabaseID),
		[]byte(viewName),
	)
	return tx.set(mappedKey, nil, []byte(sqlText))
}

func deleteView(ctx context.Context, tx *SQLTx, viewName string) error {
	mappedKey := MapKey(
		tx.sqlPrefix(),
		catalogViewPrefix,
		EncodeID(DatabaseID),
		[]byte(viewName),
	)
	return tx.delete(ctx, mappedKey)
}

func persistSequence(tx *SQLTx, seq *Sequence) error {
	mappedKey := MapKey(
		tx.sqlPrefix(),
		catalogSequencePrefix,
		EncodeID(DatabaseID),
		[]byte(seq.name),
	)

	val := make([]byte, 16)
	binary.BigEndian.PutUint64(val[0:8], uint64(seq.currValue))
	binary.BigEndian.PutUint64(val[8:16], uint64(seq.increment))

	return tx.set(mappedKey, nil, val)
}

func deleteSequence(ctx context.Context, tx *SQLTx, seqName string) error {
	mappedKey := MapKey(
		tx.sqlPrefix(),
		catalogSequencePrefix,
		EncodeID(DatabaseID),
		[]byte(seqName),
	)
	return tx.delete(ctx, mappedKey)
}

type ColSpec struct {
	colName       string
	colType       SQLValueType
	maxLen        int
	autoIncrement bool
	notNull       bool
	primaryKey    bool
	defaultValue  ValueExp
}

func NewColSpec(name string, colType SQLValueType, maxLen int, autoIncrement bool, notNull bool) *ColSpec {
	return &ColSpec{
		colName:       name,
		colType:       colType,
		maxLen:        maxLen,
		autoIncrement: autoIncrement,
		notNull:       notNull,
	}
}

type CreateIndexStmt struct {
	unique      bool
	ifNotExists bool
	table       string
	cols        []string
	predicate   ValueExp // WHERE clause for partial indexes (nil = full index)
}

func NewCreateIndexStmt(table string, cols []string, isUnique bool) *CreateIndexStmt {
	return &CreateIndexStmt{unique: isUnique, table: table, cols: cols}
}

func (stmt *CreateIndexStmt) readOnly() bool {
	return false
}

func (stmt *CreateIndexStmt) requiredPrivileges() []SQLPrivilege {
	return []SQLPrivilege{SQLPrivilegeCreate}
}

func (stmt *CreateIndexStmt) inferParameters(ctx context.Context, tx *SQLTx, params map[string]SQLValueType) error {
	return nil
}

func (stmt *CreateIndexStmt) execAt(ctx context.Context, tx *SQLTx, params map[string]interface{}) (*SQLTx, error) {
	if len(stmt.cols) < 1 {
		return nil, ErrIllegalArguments
	}

	if len(stmt.cols) > MaxNumberOfColumnsInIndex {
		return nil, ErrMaxNumberOfColumnsInIndexExceeded
	}

	table, err := tx.catalog.GetTableByName(stmt.table)
	if err != nil {
		return nil, err
	}

	colIDs := make([]uint32, len(stmt.cols))

	indexKeyLen := 0

	for i, colName := range stmt.cols {
		col, err := table.GetColumnByName(colName)
		if err != nil {
			return nil, err
		}

		if col.Type() == JSONType {
			return nil, ErrCannotIndexJson
		}

		if variableSizedType(col.colType) && !tx.engine.lazyIndexConstraintValidation && (col.MaxLen() == 0 || col.MaxLen() > MaxKeyLen) {
			return nil, fmt.Errorf("%w: can not create index using column '%s'. Max key length for variable columns is %d", ErrLimitedKeyType, col.colName, MaxKeyLen)
		}

		indexKeyLen += col.MaxLen()

		colIDs[i] = col.id
	}

	if !tx.engine.lazyIndexConstraintValidation && indexKeyLen > MaxKeyLen {
		return nil, fmt.Errorf("%w: can not create index using columns '%v'. Max key length is %d", ErrLimitedKeyType, stmt.cols, MaxKeyLen)
	}

	if stmt.unique && table.primaryIndex != nil {
		// check table is empty
		pkPrefix := MapKey(tx.sqlPrefix(), MappedPrefix, EncodeID(table.id), EncodeID(table.primaryIndex.id))
		_, _, err := tx.getWithPrefix(ctx, pkPrefix, nil)
		if errors.Is(err, store.ErrIndexNotFound) {
			return nil, ErrTableDoesNotExist
		}
		if err == nil {
			return nil, ErrLimitedIndexCreation
		} else if !errors.Is(err, store.ErrKeyNotFound) {
			return nil, err
		}
	}

	index, err := table.newIndex(stmt.unique, colIDs)
	if errors.Is(err, ErrIndexAlreadyExists) && stmt.ifNotExists {
		return tx, nil
	}
	if err != nil {
		return nil, err
	}

	// Set predicate for partial indexes
	if stmt.predicate != nil {
		index.predicate = stmt.predicate
	}

	// v={unique {colID1}(ASC|DESC)...{colIDN}(ASC|DESC)}
	// TODO: currently only ASC order is supported
	colSpecLen := EncIDLen + 1

	encodedValues := make([]byte, 1+len(index.cols)*colSpecLen)

	if index.IsUnique() {
		encodedValues[0] = 1
	}

	for i, col := range index.cols {
		copy(encodedValues[1+i*colSpecLen:], EncodeID(col.id))
	}

	mappedKey := MapKey(tx.sqlPrefix(), catalogIndexPrefix, EncodeID(DatabaseID), EncodeID(table.id), EncodeID(index.id))

	err = tx.set(mappedKey, nil, encodedValues)
	if err != nil {
		return nil, err
	}

	tx.mutatedCatalog = true

	return tx, nil
}

type AddColumnStmt struct {
	table   string
	colSpec *ColSpec
}

func NewAddColumnStmt(table string, colSpec *ColSpec) *AddColumnStmt {
	return &AddColumnStmt{table: table, colSpec: colSpec}
}

func (stmt *AddColumnStmt) readOnly() bool {
	return false
}

func (stmt *AddColumnStmt) requiredPrivileges() []SQLPrivilege {
	return []SQLPrivilege{SQLPrivilegeAlter}
}

func (stmt *AddColumnStmt) inferParameters(ctx context.Context, tx *SQLTx, params map[string]SQLValueType) error {
	return nil
}

func (stmt *AddColumnStmt) execAt(ctx context.Context, tx *SQLTx, params map[string]interface{}) (*SQLTx, error) {
	table, err := tx.catalog.GetTableByName(stmt.table)
	if err != nil {
		return nil, err
	}

	col, err := table.newColumn(stmt.colSpec)
	if err != nil {
		return nil, err
	}

	err = persistColumn(tx, col)
	if err != nil {
		return nil, err
	}

	tx.mutatedCatalog = true

	return tx, nil
}

type RenameTableStmt struct {
	oldName string
	newName string
}

func (stmt *RenameTableStmt) readOnly() bool {
	return false
}

func (stmt *RenameTableStmt) requiredPrivileges() []SQLPrivilege {
	return []SQLPrivilege{SQLPrivilegeAlter}
}

func (stmt *RenameTableStmt) inferParameters(ctx context.Context, tx *SQLTx, params map[string]SQLValueType) error {
	return nil
}

func (stmt *RenameTableStmt) execAt(ctx context.Context, tx *SQLTx, params map[string]interface{}) (*SQLTx, error) {
	table, err := tx.catalog.renameTable(stmt.oldName, stmt.newName)
	if err != nil {
		return nil, err
	}

	// update table name
	mappedKey := MapKey(
		tx.sqlPrefix(),
		catalogTablePrefix,
		EncodeID(DatabaseID),
		EncodeID(table.id),
	)
	err = tx.set(mappedKey, nil, []byte(stmt.newName))
	if err != nil {
		return nil, err
	}

	tx.mutatedCatalog = true

	return tx, nil
}

type RenameColumnStmt struct {
	table   string
	oldName string
	newName string
}

func NewRenameColumnStmt(table, oldName, newName string) *RenameColumnStmt {
	return &RenameColumnStmt{table: table, oldName: oldName, newName: newName}
}

func (stmt *RenameColumnStmt) readOnly() bool {
	return false
}

func (stmt *RenameColumnStmt) requiredPrivileges() []SQLPrivilege {
	return []SQLPrivilege{SQLPrivilegeAlter}
}

func (stmt *RenameColumnStmt) inferParameters(ctx context.Context, tx *SQLTx, params map[string]SQLValueType) error {
	return nil
}

func (stmt *RenameColumnStmt) execAt(ctx context.Context, tx *SQLTx, params map[string]interface{}) (*SQLTx, error) {
	table, err := tx.catalog.GetTableByName(stmt.table)
	if err != nil {
		return nil, err
	}

	col, err := table.renameColumn(stmt.oldName, stmt.newName)
	if err != nil {
		return nil, err
	}

	err = persistColumn(tx, col)
	if err != nil {
		return nil, err
	}

	tx.mutatedCatalog = true

	return tx, nil
}

type DropColumnStmt struct {
	table   string
	colName string
}

func NewDropColumnStmt(table, colName string) *DropColumnStmt {
	return &DropColumnStmt{table: table, colName: colName}
}

func (stmt *DropColumnStmt) readOnly() bool {
	return false
}

func (stmt *DropColumnStmt) requiredPrivileges() []SQLPrivilege {
	return []SQLPrivilege{SQLPrivilegeDrop}
}

func (stmt *DropColumnStmt) inferParameters(ctx context.Context, tx *SQLTx, params map[string]SQLValueType) error {
	return nil
}

func (stmt *DropColumnStmt) execAt(ctx context.Context, tx *SQLTx, params map[string]interface{}) (*SQLTx, error) {
	table, err := tx.catalog.GetTableByName(stmt.table)
	if err != nil {
		return nil, err
	}

	col, err := table.GetColumnByName(stmt.colName)
	if err != nil {
		return nil, err
	}

	err = canDropColumn(tx, table, col)
	if err != nil {
		return nil, err
	}

	err = table.deleteColumn(col)
	if err != nil {
		return nil, err
	}

	err = persistColumnDeletion(ctx, tx, col)
	if err != nil {
		return nil, err
	}

	tx.mutatedCatalog = true

	return tx, nil
}

func canDropColumn(tx *SQLTx, table *Table, col *Column) error {
	colSpecs := make([]*ColSpec, 0, len(table.Cols())-1)
	for _, c := range table.cols {
		if c.id != col.id {
			colSpecs = append(colSpecs, &ColSpec{colName: c.Name(), colType: c.Type()})
		}
	}

	row := zeroRow(table.Name(), colSpecs)
	for name, check := range table.checkConstraints {
		_, err := check.exp.reduce(tx, row, table.name)
		if errors.Is(err, ErrColumnDoesNotExist) {
			return fmt.Errorf("%w %s because %s constraint requires it", ErrCannotDropColumn, col.Name(), name)
		}

		if err != nil {
			return err
		}
	}
	return nil
}

func persistColumnDeletion(ctx context.Context, tx *SQLTx, col *Column) error {
	mappedKey := MapKey(
		tx.sqlPrefix(),
		catalogColumnPrefix,
		EncodeID(DatabaseID),
		EncodeID(col.table.id),
		EncodeID(col.id),
		[]byte(col.colType),
	)

	return tx.delete(ctx, mappedKey)
}

type AlterColumnAction int

const (
	AlterColumnSetNotNull  AlterColumnAction = iota
	AlterColumnDropNotNull
	AlterColumnSetType
)

type AlterColumnStmt struct {
	table   string
	colName string
	action  AlterColumnAction
	newType SQLValueType
}

func (stmt *AlterColumnStmt) readOnly() bool                     { return false }
func (stmt *AlterColumnStmt) requiredPrivileges() []SQLPrivilege { return []SQLPrivilege{SQLPrivilegeCreate} }

func (stmt *AlterColumnStmt) inferParameters(ctx context.Context, tx *SQLTx, params map[string]SQLValueType) error {
	return nil
}

func (stmt *AlterColumnStmt) execAt(ctx context.Context, tx *SQLTx, params map[string]interface{}) (*SQLTx, error) {
	table, err := tx.catalog.GetTableByName(stmt.table)
	if err != nil {
		return nil, err
	}

	col, err := table.GetColumnByName(stmt.colName)
	if err != nil {
		return nil, err
	}

	switch stmt.action {
	case AlterColumnSetNotNull:
		col.notNull = true
	case AlterColumnDropNotNull:
		// Cannot drop NOT NULL on PK columns
		if table.primaryIndex != nil {
			for _, pkCol := range table.primaryIndex.cols {
				if pkCol.id == col.id {
					return nil, fmt.Errorf("cannot drop NOT NULL constraint on primary key column %s", col.colName)
				}
			}
		}
		col.notNull = false
	case AlterColumnSetType:
		col.colType = stmt.newType
	}

	tx.mutatedCatalog = true
	return tx, nil
}

type DropConstraintStmt struct {
	table          string
	constraintName string
}

func (stmt *DropConstraintStmt) readOnly() bool {
	return false
}

func (stmt *DropConstraintStmt) requiredPrivileges() []SQLPrivilege {
	return []SQLPrivilege{SQLPrivilegeDrop}
}

func (stmt *DropConstraintStmt) execAt(ctx context.Context, tx *SQLTx, params map[string]interface{}) (*SQLTx, error) {
	table, err := tx.catalog.GetTableByName(stmt.table)
	if err != nil {
		return nil, err
	}

	id, err := table.deleteCheck(stmt.constraintName)
	if err != nil {
		return nil, err
	}

	err = persistCheckDeletion(ctx, tx, table.id, id)

	tx.mutatedCatalog = true

	return tx, err
}

func persistCheckDeletion(ctx context.Context, tx *SQLTx, tableID uint32, checkId uint32) error {
	mappedKey := MapKey(
		tx.sqlPrefix(),
		catalogCheckPrefix,
		EncodeID(DatabaseID),
		EncodeID(tableID),
		EncodeID(checkId),
	)
	return tx.delete(ctx, mappedKey)
}

func (stmt *DropConstraintStmt) inferParameters(ctx context.Context, tx *SQLTx, params map[string]SQLValueType) error {
	return nil
}

type UpsertIntoStmt struct {
	isInsert     bool
	tableRef     *tableRef
	cols         []string
	ds           DataSource
	onConflict   *OnConflictDo
	returnedRows []*Row // populated during execAt for RETURNING
}

func (stmt *UpsertIntoStmt) readOnly() bool {
	return false
}

func (stmt *UpsertIntoStmt) requiredPrivileges() []SQLPrivilege {
	privileges := stmt.privileges()
	if stmt.ds != nil {
		privileges = append(privileges, stmt.ds.requiredPrivileges()...)
	}
	return privileges
}

func (stmt *UpsertIntoStmt) privileges() []SQLPrivilege {
	if stmt.isInsert {
		return []SQLPrivilege{SQLPrivilegeInsert}
	}
	return []SQLPrivilege{SQLPrivilegeInsert, SQLPrivilegeUpdate}
}

func NewUpsertIntoStmt(table string, cols []string, ds DataSource, isInsert bool, onConflict *OnConflictDo) *UpsertIntoStmt {
	return &UpsertIntoStmt{
		isInsert:   isInsert,
		tableRef:   NewTableRef(table, ""),
		cols:       cols,
		ds:         ds,
		onConflict: onConflict,
	}
}

type RowSpec struct {
	Values []ValueExp
}

func NewRowSpec(values []ValueExp) *RowSpec {
	return &RowSpec{
		Values: values,
	}
}

type OnConflictDo struct {
	updates []*colUpdate // nil means DO NOTHING, non-nil means DO UPDATE SET ...
}

func (stmt *UpsertIntoStmt) inferParameters(ctx context.Context, tx *SQLTx, params map[string]SQLValueType) error {
	ds, ok := stmt.ds.(*valuesDataSource)
	if !ok {
		return stmt.ds.inferParameters(ctx, tx, params)
	}

	emptyDescriptors := make(map[string]ColDescriptor)
	for _, row := range ds.rows {
		if len(stmt.cols) != len(row.Values) {
			return ErrInvalidNumberOfValues
		}

		for i, val := range row.Values {
			table, err := stmt.tableRef.referencedTable(tx)
			if err != nil {
				return err
			}

			col, err := table.GetColumnByName(stmt.cols[i])
			if err != nil {
				return err
			}

			err = val.requiresType(col.colType, emptyDescriptors, params, table.name)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (stmt *UpsertIntoStmt) validate(table *Table) (map[uint32]int, error) {
	selPosByColID := make(map[uint32]int, len(stmt.cols))

	for i, c := range stmt.cols {
		col, err := table.GetColumnByName(c)
		if err != nil {
			return nil, err
		}

		_, duplicated := selPosByColID[col.id]
		if duplicated {
			return nil, fmt.Errorf("%w (%s)", ErrDuplicatedColumn, col.colName)
		}

		selPosByColID[col.id] = i
	}

	return selPosByColID, nil
}

func (stmt *UpsertIntoStmt) execAt(ctx context.Context, tx *SQLTx, params map[string]interface{}) (*SQLTx, error) {
	table, err := stmt.tableRef.referencedTable(tx)
	if err != nil {
		return nil, err
	}

	selPosByColID, err := stmt.validate(table)
	if err != nil {
		return nil, err
	}

	r := &Row{
		ValuesByPosition: make([]TypedValue, len(table.cols)),
		ValuesBySelector: make(map[string]TypedValue),
	}

	reader, err := stmt.ds.Resolve(ctx, tx, params, nil)
	if err != nil {
		return nil, err
	}
	defer reader.Close()

	for {
		row, err := reader.Read(ctx)
		if errors.Is(err, ErrNoMoreRows) {
			break
		}
		if err != nil {
			return nil, err
		}

		if len(row.ValuesByPosition) != len(stmt.cols) {
			return nil, ErrInvalidNumberOfValues
		}

		valuesByColID := make(map[uint32]TypedValue)

		var pkMustExist bool

		for colID, col := range table.colsByID {
			colPos, specified := selPosByColID[colID]
			if !specified {
				// Use default value if defined
				if col.HasDefault() {
					defVal, err := col.DefaultValue().reduce(tx, nil, table.name)
					if err != nil {
						return nil, fmt.Errorf("error evaluating default for column '%s': %w", col.colName, err)
					}
					if !defVal.IsNull() {
						valuesByColID[colID] = defVal
					}
					continue
				}

				if col.notNull && !col.autoIncrement {
					return nil, fmt.Errorf("%w (%s)", ErrNotNullableColumnCannotBeNull, col.colName)
				}

				// inject auto-incremental pk value
				if stmt.isInsert && col.autoIncrement {
					// current implementation assumes only PK can be set as autoincremental
					table.maxPK++

					pkCol := table.primaryIndex.cols[0]
					valuesByColID[pkCol.id] = &Integer{val: table.maxPK}

					if _, ok := tx.firstInsertedPKs[table.name]; !ok {
						tx.firstInsertedPKs[table.name] = table.maxPK
					}
					tx.lastInsertedPKs[table.name] = table.maxPK
				}

				continue
			}

			// value was specified
			cVal := row.ValuesByPosition[colPos]

			val, err := cVal.substitute(params)
			if err != nil {
				return nil, err
			}

			rval, err := val.reduce(tx, nil, table.name)
			if err != nil {
				return nil, err
			}

			if rval.IsNull() {
				if col.notNull || col.autoIncrement {
					return nil, fmt.Errorf("%w (%s)", ErrNotNullableColumnCannotBeNull, col.colName)
				}

				continue
			}

			if col.autoIncrement {
				// validate specified value
				nl, isNumber := rval.RawValue().(int64)
				if !isNumber {
					return nil, fmt.Errorf("%w (expecting numeric value)", ErrInvalidValue)
				}

				pkMustExist = nl <= table.maxPK

				if _, ok := tx.firstInsertedPKs[table.name]; !ok {
					tx.firstInsertedPKs[table.name] = nl
				}
				tx.lastInsertedPKs[table.name] = nl
			}

			valuesByColID[colID] = rval
		}

		for i, col := range table.cols {
			v := valuesByColID[col.id]

			if v == nil {
				v = NewNull(AnyType)
			} else if len(table.checkConstraints) > 0 && col.Type() == JSONType {
				s, _ := v.RawValue().(string)
				jsonVal, err := NewJsonFromString(s)
				if err != nil {
					return nil, err
				}
				v = jsonVal
			}

			r.ValuesByPosition[i] = v
			r.ValuesBySelector[EncodeSelector("", table.name, col.colName)] = v
		}

		if err := checkConstraints(tx, table.checkConstraints, r, table.name); err != nil {
			return nil, err
		}

		pkEncVals, err := encodedKey(table.primaryIndex, valuesByColID)
		if err != nil {
			return nil, err
		}

		// pk entry
		mappedPKey := MapKey(tx.sqlPrefix(), MappedPrefix, EncodeID(table.id), EncodeID(table.primaryIndex.id), pkEncVals, pkEncVals)
		if len(mappedPKey) > MaxKeyLen {
			return nil, ErrMaxKeyLengthExceeded
		}

		_, err = tx.get(ctx, mappedPKey)
		if err != nil && !errors.Is(err, store.ErrKeyNotFound) {
			return nil, err
		}

		if errors.Is(err, store.ErrKeyNotFound) && pkMustExist {
			return nil, fmt.Errorf("%w: specified value must be greater than current one", ErrInvalidValue)
		}

		if stmt.isInsert {
			if err == nil && stmt.onConflict == nil {
				return nil, store.ErrKeyAlreadyExists
			}

			if err == nil && stmt.onConflict != nil {
				if stmt.onConflict.updates == nil {
					// ON CONFLICT DO NOTHING
					continue
				}

				// ON CONFLICT DO UPDATE SET ...
				for _, u := range stmt.onConflict.updates {
					col, colExists := table.colsByName[u.col]
					if !colExists {
						return nil, fmt.Errorf("%w (%s)", ErrColumnDoesNotExist, u.col)
					}

					uval, err := u.val.substitute(params)
					if err != nil {
						return nil, err
					}

					rval, err := uval.reduce(tx, r, table.name)
					if err != nil {
						return nil, err
					}

					valuesByColID[col.id] = rval

					// update row representation for check constraints
					for i, c := range table.cols {
						if c.id == col.id {
							r.ValuesByPosition[i] = rval
							r.ValuesBySelector[EncodeSelector("", table.name, c.colName)] = rval
							break
						}
					}
				}
			}
		}

		err = tx.doUpsert(ctx, pkEncVals, valuesByColID, table, !stmt.isInsert)
		if err != nil {
			return nil, err
		}

		// Capture row for RETURNING clause
		capturedRow := &Row{
			ValuesByPosition: make([]TypedValue, len(r.ValuesByPosition)),
			ValuesBySelector: make(map[string]TypedValue, len(r.ValuesBySelector)),
		}
		copy(capturedRow.ValuesByPosition, r.ValuesByPosition)
		for k, v := range r.ValuesBySelector {
			capturedRow.ValuesBySelector[k] = v
		}
		stmt.returnedRows = append(stmt.returnedRows, capturedRow)
	}
	return tx, nil
}

func checkConstraints(tx *SQLTx, checks map[string]CheckConstraint, row *Row, table string) error {
	for _, check := range checks {
		val, err := check.exp.reduce(tx, row, table)
		if err != nil {
			return fmt.Errorf("%w: %s", ErrCheckConstraintViolation, err)
		}

		if val.Type() != BooleanType {
			return ErrInvalidCheckConstraint
		}

		if !val.RawValue().(bool) {
			return fmt.Errorf("%w: %s", ErrCheckConstraintViolation, check.exp.String())
		}
	}
	return nil
}

func (tx *SQLTx) encodeRowValue(valuesByColID map[uint32]TypedValue, table *Table) ([]byte, error) {
	valbuf := bytes.Buffer{}

	// null values are not serialized
	encodedVals := 0
	for _, v := range valuesByColID {
		if !v.IsNull() {
			encodedVals++
		}
	}

	b := make([]byte, EncLenLen)
	binary.BigEndian.PutUint32(b, uint32(encodedVals))

	_, err := valbuf.Write(b)
	if err != nil {
		return nil, err
	}

	for _, col := range table.cols {
		rval, specified := valuesByColID[col.id]
		if !specified || rval.IsNull() {
			continue
		}

		b := make([]byte, EncIDLen)
		binary.BigEndian.PutUint32(b, uint32(col.id))

		_, err = valbuf.Write(b)
		if err != nil {
			return nil, fmt.Errorf("%w: table: %s, column: %s", err, table.name, col.colName)
		}

		encVal, err := EncodeValue(rval, col.colType, col.MaxLen())
		if err != nil {
			return nil, fmt.Errorf("%w: table: %s, column: %s", err, table.name, col.colName)
		}

		_, err = valbuf.Write(encVal)
		if err != nil {
			return nil, fmt.Errorf("%w: table: %s, column: %s", err, table.name, col.colName)
		}
	}

	return valbuf.Bytes(), nil
}

func (tx *SQLTx) doUpsert(ctx context.Context, pkEncVals []byte, valuesByColID map[uint32]TypedValue, table *Table, reuseIndex bool) error {
	var reusableIndexEntries map[uint32]struct{}

	if reuseIndex && len(table.indexes) > 1 {
		currPKRow, err := tx.fetchPKRow(ctx, table, valuesByColID)
		if err == nil {
			currValuesByColID := make(map[uint32]TypedValue, len(currPKRow.ValuesBySelector))

			for _, col := range table.cols {
				encSel := EncodeSelector("", table.name, col.colName)
				currValuesByColID[col.id] = currPKRow.ValuesBySelector[encSel]
			}

			reusableIndexEntries, err = tx.deprecateIndexEntries(pkEncVals, currValuesByColID, valuesByColID, table)
			if err != nil {
				return err
			}
		} else if !errors.Is(err, ErrNoMoreRows) {
			return err
		}
	}

	rowKey := MapKey(tx.sqlPrefix(), RowPrefix, EncodeID(DatabaseID), EncodeID(table.id), EncodeID(PKIndexID), pkEncVals)

	encodedRowValue, err := tx.encodeRowValue(valuesByColID, table)
	if err != nil {
		return err
	}

	err = tx.set(rowKey, nil, encodedRowValue)
	if err != nil {
		return err
	}

	// create in-memory and validate entries for secondary indexes
	for _, index := range table.indexes {
		if index.IsPrimary() {
			continue
		}

		if reusableIndexEntries != nil {
			_, reusable := reusableIndexEntries[index.id]
			if reusable {
				continue
			}
		}

		encodedValues := make([][]byte, 2+len(index.cols))
		encodedValues[0] = EncodeID(table.id)
		encodedValues[1] = EncodeID(index.id)

		indexKeyLen := 0

		for i, col := range index.cols {
			rval, specified := valuesByColID[col.id]
			if !specified {
				rval = &NullValue{t: col.colType}
			}

			encVal, n, err := EncodeValueAsKey(rval, col.colType, col.MaxLen())
			if err != nil {
				return fmt.Errorf("%w: index on '%s' and column '%s'", err, index.Name(), col.colName)
			}

			if n > MaxKeyLen {
				return fmt.Errorf("%w: can not index entry for column '%s'. Max key length for variable columns is %d", ErrLimitedKeyType, col.colName, MaxKeyLen)
			}

			indexKeyLen += n

			encodedValues[i+2] = encVal
		}

		if indexKeyLen > MaxKeyLen {
			return fmt.Errorf("%w: can not index entry using columns '%v'. Max key length is %d", ErrLimitedKeyType, index.cols, MaxKeyLen)
		}

		smkey := MapKey(tx.sqlPrefix(), MappedPrefix, encodedValues...)

		// no other equivalent entry should be already indexed
		if index.IsUnique() {
			_, valRef, err := tx.getWithPrefix(ctx, smkey, nil)
			if err == nil && (valRef.KVMetadata() == nil || !valRef.KVMetadata().Deleted()) {
				return store.ErrKeyAlreadyExists
			} else if !errors.Is(err, store.ErrKeyNotFound) {
				return err
			}
		}

		err = tx.setTransient(smkey, nil, encodedRowValue) // only-indexable
		if err != nil {
			return err
		}
	}

	tx.updatedRows++

	return nil
}

func encodedKey(index *Index, valuesByColID map[uint32]TypedValue) ([]byte, error) {
	valbuf := bytes.Buffer{}

	indexKeyLen := 0

	for _, col := range index.cols {
		rval, specified := valuesByColID[col.id]
		if !specified || rval.IsNull() {
			return nil, ErrPKCanNotBeNull
		}

		encVal, n, err := EncodeValueAsKey(rval, col.colType, col.MaxLen())
		if err != nil {
			return nil, fmt.Errorf("%w: index of table '%s' and column '%s'", err, index.table.name, col.colName)
		}

		if n > MaxKeyLen {
			return nil, fmt.Errorf("%w: invalid key entry for column '%s'. Max key length for variable columns is %d", ErrLimitedKeyType, col.colName, MaxKeyLen)
		}

		indexKeyLen += n

		_, err = valbuf.Write(encVal)
		if err != nil {
			return nil, err
		}
	}

	if indexKeyLen > MaxKeyLen {
		return nil, fmt.Errorf("%w: invalid key entry using columns '%v'. Max key length is %d", ErrLimitedKeyType, index.cols, MaxKeyLen)
	}

	return valbuf.Bytes(), nil
}

func (tx *SQLTx) fetchPKRow(ctx context.Context, table *Table, valuesByColID map[uint32]TypedValue) (*Row, error) {
	pkRanges := make(map[uint32]*typedValueRange, len(table.primaryIndex.cols))

	for _, pkCol := range table.primaryIndex.cols {
		pkVal := valuesByColID[pkCol.id]

		pkRanges[pkCol.id] = &typedValueRange{
			lRange: &typedValueSemiRange{val: pkVal, inclusive: true},
			hRange: &typedValueSemiRange{val: pkVal, inclusive: true},
		}
	}

	scanSpecs := &ScanSpecs{
		Index:         table.primaryIndex,
		rangesByColID: pkRanges,
	}

	r, err := newRawRowReader(tx, nil, table, period{}, table.name, scanSpecs)
	if err != nil {
		return nil, err
	}

	defer func() {
		r.Close()
	}()

	return r.Read(ctx)
}

// deprecateIndexEntries mark previous index entries as deleted
func (tx *SQLTx) deprecateIndexEntries(
	pkEncVals []byte,
	currValuesByColID, newValuesByColID map[uint32]TypedValue,
	table *Table) (reusableIndexEntries map[uint32]struct{}, err error) {

	encodedRowValue, err := tx.encodeRowValue(currValuesByColID, table)
	if err != nil {
		return nil, err
	}

	reusableIndexEntries = make(map[uint32]struct{})

	for _, index := range table.indexes {
		if index.IsPrimary() {
			continue
		}

		encodedValues := make([][]byte, 2+len(index.cols)+1)
		encodedValues[0] = EncodeID(table.id)
		encodedValues[1] = EncodeID(index.id)
		encodedValues[len(encodedValues)-1] = pkEncVals

		// existent index entry is deleted only if it differs from existent one
		sameIndexKey := true

		for i, col := range index.cols {
			currVal, specified := currValuesByColID[col.id]
			if !specified {
				currVal = &NullValue{t: col.colType}
			}

			newVal, specified := newValuesByColID[col.id]
			if !specified {
				newVal = &NullValue{t: col.colType}
			}

			r, err := currVal.Compare(newVal)
			if err != nil {
				return nil, err
			}

			sameIndexKey = sameIndexKey && r == 0

			encVal, _, _ := EncodeValueAsKey(currVal, col.colType, col.MaxLen())

			encodedValues[i+3] = encVal
		}

		// mark existent index entry as deleted
		if sameIndexKey {
			reusableIndexEntries[index.id] = struct{}{}
		} else {
			md := store.NewKVMetadata()

			md.AsDeleted(true)

			err = tx.set(MapKey(tx.sqlPrefix(), MappedPrefix, encodedValues...), md, encodedRowValue)
			if err != nil {
				return nil, err
			}
		}
	}

	return reusableIndexEntries, nil
}

type UpdateStmt struct {
	tableRef     *tableRef
	where        ValueExp
	updates      []*colUpdate
	indexOn      []string
	limit        ValueExp
	offset       ValueExp
	returnedRows []*Row
}

type colUpdate struct {
	col string
	op  CmpOperator
	val ValueExp
}

func (stmt *UpdateStmt) readOnly() bool {
	return false
}

func (stmt *UpdateStmt) requiredPrivileges() []SQLPrivilege {
	return []SQLPrivilege{SQLPrivilegeUpdate}
}

func (stmt *UpdateStmt) inferParameters(ctx context.Context, tx *SQLTx, params map[string]SQLValueType) error {
	selectStmt := &SelectStmt{
		ds:    stmt.tableRef,
		where: stmt.where,
	}

	err := selectStmt.inferParameters(ctx, tx, params)
	if err != nil {
		return err
	}

	table, err := stmt.tableRef.referencedTable(tx)
	if err != nil {
		return err
	}

	for _, update := range stmt.updates {
		col, err := table.GetColumnByName(update.col)
		if err != nil {
			return err
		}

		err = update.val.requiresType(col.colType, make(map[string]ColDescriptor), params, table.name)
		if err != nil {
			return err
		}
	}

	return nil
}

func (stmt *UpdateStmt) validate(table *Table) error {
	colIDs := make(map[uint32]struct{}, len(stmt.updates))

	for _, update := range stmt.updates {
		if update.op != EQ {
			return ErrIllegalArguments
		}

		col, err := table.GetColumnByName(update.col)
		if err != nil {
			return err
		}

		if table.PrimaryIndex().IncludesCol(col.id) {
			return ErrPKCanNotBeUpdated
		}

		_, duplicated := colIDs[col.id]
		if duplicated {
			return ErrDuplicatedColumn
		}

		colIDs[col.id] = struct{}{}
	}

	return nil
}

func (stmt *UpdateStmt) execAt(ctx context.Context, tx *SQLTx, params map[string]interface{}) (*SQLTx, error) {
	selectStmt := &SelectStmt{
		ds:      stmt.tableRef,
		where:   stmt.where,
		indexOn: stmt.indexOn,
		limit:   stmt.limit,
		offset:  stmt.offset,
	}

	rowReader, err := selectStmt.Resolve(ctx, tx, params, nil)
	if err != nil {
		return nil, err
	}
	defer rowReader.Close()

	table := rowReader.ScanSpecs().Index.table

	err = stmt.validate(table)
	if err != nil {
		return nil, err
	}

	cols, err := rowReader.colsBySelector(ctx)
	if err != nil {
		return nil, err
	}

	for {
		row, err := rowReader.Read(ctx)
		if errors.Is(err, ErrNoMoreRows) {
			break
		} else if err != nil {
			return nil, err
		}

		valuesByColID := make(map[uint32]TypedValue, len(row.ValuesBySelector))

		for _, col := range table.cols {
			encSel := EncodeSelector("", table.name, col.colName)
			valuesByColID[col.id] = row.ValuesBySelector[encSel]
		}

		for _, update := range stmt.updates {
			col, err := table.GetColumnByName(update.col)
			if err != nil {
				return nil, err
			}

			sval, err := update.val.substitute(params)
			if err != nil {
				return nil, err
			}

			rval, err := sval.reduce(tx, row, table.name)
			if err != nil {
				return nil, err
			}

			err = rval.requiresType(col.colType, cols, nil, table.name)
			if err != nil {
				return nil, err
			}

			valuesByColID[col.id] = rval
		}

		for i, col := range table.cols {
			v := valuesByColID[col.id]

			row.ValuesByPosition[i] = v
			row.ValuesBySelector[EncodeSelector("", table.name, col.colName)] = v
		}

		if err := checkConstraints(tx, table.checkConstraints, row, table.name); err != nil {
			return nil, err
		}

		pkEncVals, err := encodedKey(table.primaryIndex, valuesByColID)
		if err != nil {
			return nil, err
		}

		// primary index entry
		mkey := MapKey(tx.sqlPrefix(), MappedPrefix, EncodeID(table.id), EncodeID(table.primaryIndex.id), pkEncVals, pkEncVals)

		// mkey must exist
		_, err = tx.get(ctx, mkey)
		if err != nil {
			return nil, err
		}

		err = tx.doUpsert(ctx, pkEncVals, valuesByColID, table, true)
		if err != nil {
			return nil, err
		}

		// Capture row for RETURNING clause
		capturedRow := &Row{
			ValuesByPosition: make([]TypedValue, len(row.ValuesByPosition)),
			ValuesBySelector: make(map[string]TypedValue, len(row.ValuesBySelector)),
		}
		copy(capturedRow.ValuesByPosition, row.ValuesByPosition)
		for k, v := range row.ValuesBySelector {
			capturedRow.ValuesBySelector[k] = v
		}
		stmt.returnedRows = append(stmt.returnedRows, capturedRow)
	}

	return tx, nil
}

type DeleteFromStmt struct {
	tableRef     *tableRef
	where        ValueExp
	indexOn      []string
	orderBy      []*OrdExp
	limit        ValueExp
	offset       ValueExp
	returnedRows []*Row
}

func NewDeleteFromStmt(table string, where ValueExp, orderBy []*OrdExp, limit ValueExp) *DeleteFromStmt {
	return &DeleteFromStmt{
		tableRef: NewTableRef(table, ""),
		where:    where,
		orderBy:  orderBy,
		limit:    limit,
	}
}

func (stmt *DeleteFromStmt) readOnly() bool {
	return false
}

func (stmt *DeleteFromStmt) requiredPrivileges() []SQLPrivilege {
	return []SQLPrivilege{SQLPrivilegeDelete}
}

func (stmt *DeleteFromStmt) inferParameters(ctx context.Context, tx *SQLTx, params map[string]SQLValueType) error {
	selectStmt := &SelectStmt{
		ds:      stmt.tableRef,
		where:   stmt.where,
		orderBy: stmt.orderBy,
	}
	return selectStmt.inferParameters(ctx, tx, params)
}

func (stmt *DeleteFromStmt) execAt(ctx context.Context, tx *SQLTx, params map[string]interface{}) (*SQLTx, error) {
	selectStmt := &SelectStmt{
		ds:      stmt.tableRef,
		where:   stmt.where,
		indexOn: stmt.indexOn,
		orderBy: stmt.orderBy,
		limit:   stmt.limit,
		offset:  stmt.offset,
	}

	rowReader, err := selectStmt.Resolve(ctx, tx, params, nil)
	if err != nil {
		return nil, err
	}
	defer rowReader.Close()

	table := rowReader.ScanSpecs().Index.table

	for {
		row, err := rowReader.Read(ctx)
		if errors.Is(err, ErrNoMoreRows) {
			break
		}
		if err != nil {
			return nil, err
		}

		valuesByColID := make(map[uint32]TypedValue, len(row.ValuesBySelector))

		for _, col := range table.cols {
			encSel := EncodeSelector("", table.name, col.colName)
			valuesByColID[col.id] = row.ValuesBySelector[encSel]
		}

		pkEncVals, err := encodedKey(table.primaryIndex, valuesByColID)
		if err != nil {
			return nil, err
		}

		// Capture row for RETURNING clause (before deletion)
		capturedRow := &Row{
			ValuesByPosition: make([]TypedValue, len(row.ValuesByPosition)),
			ValuesBySelector: make(map[string]TypedValue, len(row.ValuesBySelector)),
		}
		copy(capturedRow.ValuesByPosition, row.ValuesByPosition)
		for k, v := range row.ValuesBySelector {
			capturedRow.ValuesBySelector[k] = v
		}
		stmt.returnedRows = append(stmt.returnedRows, capturedRow)

		err = tx.deleteIndexEntries(pkEncVals, valuesByColID, table)
		if err != nil {
			return nil, err
		}

		tx.updatedRows++
	}
	return tx, nil
}

func (tx *SQLTx) deleteIndexEntries(pkEncVals []byte, valuesByColID map[uint32]TypedValue, table *Table) error {
	encodedRowValue, err := tx.encodeRowValue(valuesByColID, table)
	if err != nil {
		return err
	}

	for _, index := range table.indexes {
		if !index.IsPrimary() {
			continue
		}

		encodedValues := make([][]byte, 3+len(index.cols))
		encodedValues[0] = EncodeID(DatabaseID)
		encodedValues[1] = EncodeID(table.id)
		encodedValues[2] = EncodeID(index.id)

		for i, col := range index.cols {
			val, specified := valuesByColID[col.id]
			if !specified {
				val = &NullValue{t: col.colType}
			}

			encVal, _, _ := EncodeValueAsKey(val, col.colType, col.MaxLen())

			encodedValues[i+3] = encVal
		}

		md := store.NewKVMetadata()

		md.AsDeleted(true)

		err := tx.set(MapKey(tx.sqlPrefix(), RowPrefix, encodedValues...), md, encodedRowValue)
		if err != nil {
			return err
		}
	}

	return nil
}

type ValueExp interface {
	inferType(cols map[string]ColDescriptor, params map[string]SQLValueType, implicitTable string) (SQLValueType, error)
	requiresType(t SQLValueType, cols map[string]ColDescriptor, params map[string]SQLValueType, implicitTable string) error
	substitute(params map[string]interface{}) (ValueExp, error)
	selectors() []Selector
	reduce(tx *SQLTx, row *Row, implicitTable string) (TypedValue, error)
	reduceSelectors(row *Row, implicitTable string) ValueExp
	isConstant() bool
	selectorRanges(table *Table, asTable string, params map[string]interface{}, rangesByColID map[uint32]*typedValueRange) error
	String() string
}

type typedValueRange struct {
	lRange *typedValueSemiRange
	hRange *typedValueSemiRange
}

type typedValueSemiRange struct {
	val       TypedValue
	inclusive bool
}

func (r *typedValueRange) unitary() bool {
	// TODO: this simplified implementation doesn't cover all unitary cases e.g. 3<=v<4
	if r.lRange == nil || r.hRange == nil {
		return false
	}

	res, _ := r.lRange.val.Compare(r.hRange.val)
	return res == 0 && r.lRange.inclusive && r.hRange.inclusive
}

func (r *typedValueRange) refineWith(refiningRange *typedValueRange) error {
	if r.lRange == nil {
		r.lRange = refiningRange.lRange
	} else if r.lRange != nil && refiningRange.lRange != nil {
		maxRange, err := maxSemiRange(r.lRange, refiningRange.lRange)
		if err != nil {
			return err
		}
		r.lRange = maxRange
	}

	if r.hRange == nil {
		r.hRange = refiningRange.hRange
	} else if r.hRange != nil && refiningRange.hRange != nil {
		minRange, err := minSemiRange(r.hRange, refiningRange.hRange)
		if err != nil {
			return err
		}
		r.hRange = minRange
	}

	return nil
}

func (r *typedValueRange) extendWith(extendingRange *typedValueRange) error {
	if r.lRange == nil || extendingRange.lRange == nil {
		r.lRange = nil
	} else {
		minRange, err := minSemiRange(r.lRange, extendingRange.lRange)
		if err != nil {
			return err
		}
		r.lRange = minRange
	}

	if r.hRange == nil || extendingRange.hRange == nil {
		r.hRange = nil
	} else {
		maxRange, err := maxSemiRange(r.hRange, extendingRange.hRange)
		if err != nil {
			return err
		}
		r.hRange = maxRange
	}

	return nil
}

func maxSemiRange(or1, or2 *typedValueSemiRange) (*typedValueSemiRange, error) {
	r, err := or1.val.Compare(or2.val)
	if err != nil {
		return nil, err
	}

	maxVal := or1.val
	if r < 0 {
		maxVal = or2.val
	}

	return &typedValueSemiRange{
		val:       maxVal,
		inclusive: or1.inclusive && or2.inclusive,
	}, nil
}

func minSemiRange(or1, or2 *typedValueSemiRange) (*typedValueSemiRange, error) {
	r, err := or1.val.Compare(or2.val)
	if err != nil {
		return nil, err
	}

	minVal := or1.val
	if r > 0 {
		minVal = or2.val
	}

	return &typedValueSemiRange{
		val:       minVal,
		inclusive: or1.inclusive || or2.inclusive,
	}, nil
}

type TypedValue interface {
	ValueExp
	Type() SQLValueType
	RawValue() interface{}
	Compare(val TypedValue) (int, error)
	IsNull() bool
}

type Tuple []TypedValue

func (t Tuple) Compare(other Tuple) (int, int, error) {
	if len(t) != len(other) {
		return -1, -1, ErrNotComparableValues
	}

	for i := range t {
		res, err := t[i].Compare(other[i])
		if err != nil || res != 0 {
			return res, i, err
		}
	}
	return 0, -1, nil
}

func NewNull(t SQLValueType) *NullValue {
	return &NullValue{t: t}
}

type NullValue struct {
	t SQLValueType
}

func (n *NullValue) Type() SQLValueType {
	return n.t
}

func (n *NullValue) RawValue() interface{} {
	return nil
}

func (n *NullValue) IsNull() bool {
	return true
}

func (n *NullValue) String() string {
	return "NULL"
}

func (n *NullValue) Compare(val TypedValue) (int, error) {
	if n.t != AnyType && val.Type() != AnyType && n.t != val.Type() {
		return 0, ErrNotComparableValues
	}

	if val.RawValue() == nil {
		return 0, nil
	}
	return -1, nil
}

func (v *NullValue) inferType(cols map[string]ColDescriptor, params map[string]SQLValueType, implicitTable string) (SQLValueType, error) {
	return v.t, nil
}

func (v *NullValue) requiresType(t SQLValueType, cols map[string]ColDescriptor, params map[string]SQLValueType, implicitTable string) error {
	if v.t == t {
		return nil
	}

	if v.t != AnyType {
		return ErrInvalidTypes
	}

	v.t = t

	return nil
}

func (v *NullValue) selectors() []Selector {
	return nil
}

func (v *NullValue) substitute(params map[string]interface{}) (ValueExp, error) {
	return v, nil
}

func (v *NullValue) reduce(tx *SQLTx, row *Row, implicitTable string) (TypedValue, error) {
	return v, nil
}

func (v *NullValue) reduceSelectors(row *Row, implicitTable string) ValueExp {
	return v
}

func (v *NullValue) isConstant() bool {
	return true
}

func (v *NullValue) selectorRanges(table *Table, asTable string, params map[string]interface{}, rangesByColID map[uint32]*typedValueRange) error {
	return nil
}

type Integer struct {
	val int64
}

func NewInteger(val int64) *Integer {
	return &Integer{val: val}
}

func (v *Integer) Type() SQLValueType {
	return IntegerType
}

func (v *Integer) IsNull() bool {
	return false
}

func (v *Integer) String() string {
	return strconv.FormatInt(v.val, 10)
}

func (v *Integer) inferType(cols map[string]ColDescriptor, params map[string]SQLValueType, implicitTable string) (SQLValueType, error) {
	return IntegerType, nil
}

func (v *Integer) requiresType(t SQLValueType, cols map[string]ColDescriptor, params map[string]SQLValueType, implicitTable string) error {
	if t != IntegerType && t != JSONType {
		return fmt.Errorf("%w: %v can not be interpreted as type %v", ErrInvalidTypes, IntegerType, t)
	}
	return nil
}

func (v *Integer) selectors() []Selector {
	return nil
}

func (v *Integer) substitute(params map[string]interface{}) (ValueExp, error) {
	return v, nil
}

func (v *Integer) reduce(tx *SQLTx, row *Row, implicitTable string) (TypedValue, error) {
	return v, nil
}

func (v *Integer) reduceSelectors(row *Row, implicitTable string) ValueExp {
	return v
}

func (v *Integer) isConstant() bool {
	return true
}

func (v *Integer) selectorRanges(table *Table, asTable string, params map[string]interface{}, rangesByColID map[uint32]*typedValueRange) error {
	return nil
}

func (v *Integer) RawValue() interface{} {
	return v.val
}

func (v *Integer) Compare(val TypedValue) (int, error) {
	if val.IsNull() {
		return 1, nil
	}

	if val.Type() == JSONType {
		res, err := val.Compare(v)
		return -res, err
	}

	if val.Type() == Float64Type {
		r, err := val.Compare(v)
		return r * -1, err
	}

	if val.Type() != IntegerType {
		return 0, ErrNotComparableValues
	}

	rval := val.RawValue().(int64)

	if v.val == rval {
		return 0, nil
	}

	if v.val > rval {
		return 1, nil
	}

	return -1, nil
}

type Timestamp struct {
	val time.Time
}

func (v *Timestamp) Type() SQLValueType {
	return TimestampType
}

func (v *Timestamp) IsNull() bool {
	return false
}

func (v *Timestamp) String() string {
	return v.val.Format("2006-01-02 15:04:05.999999")
}

func (v *Timestamp) inferType(cols map[string]ColDescriptor, params map[string]SQLValueType, implicitTable string) (SQLValueType, error) {
	return TimestampType, nil
}

func (v *Timestamp) requiresType(t SQLValueType, cols map[string]ColDescriptor, params map[string]SQLValueType, implicitTable string) error {
	if t != TimestampType {
		return fmt.Errorf("%w: %v can not be interpreted as type %v", ErrInvalidTypes, TimestampType, t)
	}

	return nil
}

func (v *Timestamp) selectors() []Selector {
	return nil
}

func (v *Timestamp) substitute(params map[string]interface{}) (ValueExp, error) {
	return v, nil
}

func (v *Timestamp) reduce(tx *SQLTx, row *Row, implicitTable string) (TypedValue, error) {
	return v, nil
}

func (v *Timestamp) reduceSelectors(row *Row, implicitTable string) ValueExp {
	return v
}

func (v *Timestamp) isConstant() bool {
	return true
}

func (v *Timestamp) selectorRanges(table *Table, asTable string, params map[string]interface{}, rangesByColID map[uint32]*typedValueRange) error {
	return nil
}

func (v *Timestamp) RawValue() interface{} {
	return v.val
}

func (v *Timestamp) Compare(val TypedValue) (int, error) {
	if val.IsNull() {
		return 1, nil
	}

	if val.Type() != TimestampType {
		return 0, ErrNotComparableValues
	}

	rval := val.RawValue().(time.Time)

	if v.val.Before(rval) {
		return -1, nil
	}

	if v.val.After(rval) {
		return 1, nil
	}

	return 0, nil
}

type Varchar struct {
	val string
}

func NewVarchar(val string) *Varchar {
	return &Varchar{val: val}
}

func (v *Varchar) Type() SQLValueType {
	return VarcharType
}

func (v *Varchar) IsNull() bool {
	return false
}

func (v *Varchar) String() string {
	return fmt.Sprintf("'%s'", v.val)
}

func (v *Varchar) inferType(cols map[string]ColDescriptor, params map[string]SQLValueType, implicitTable string) (SQLValueType, error) {
	return VarcharType, nil
}

func (v *Varchar) requiresType(t SQLValueType, cols map[string]ColDescriptor, params map[string]SQLValueType, implicitTable string) error {
	if t != VarcharType && t != JSONType {
		return fmt.Errorf("%w: %v can not be interpreted as type %v", ErrInvalidTypes, VarcharType, t)
	}
	return nil
}

func (v *Varchar) selectors() []Selector {
	return nil
}

func (v *Varchar) substitute(params map[string]interface{}) (ValueExp, error) {
	return v, nil
}

func (v *Varchar) reduce(tx *SQLTx, row *Row, implicitTable string) (TypedValue, error) {
	return v, nil
}

func (v *Varchar) reduceSelectors(row *Row, implicitTable string) ValueExp {
	return v
}

func (v *Varchar) isConstant() bool {
	return true
}

func (v *Varchar) selectorRanges(table *Table, asTable string, params map[string]interface{}, rangesByColID map[uint32]*typedValueRange) error {
	return nil
}

func (v *Varchar) RawValue() interface{} {
	return v.val
}

func (v *Varchar) Compare(val TypedValue) (int, error) {
	if val.IsNull() {
		return 1, nil
	}

	if val.Type() == JSONType {
		res, err := val.Compare(v)
		return -res, err
	}

	if val.Type() != VarcharType {
		return 0, ErrNotComparableValues
	}

	rval := val.RawValue().(string)

	return bytes.Compare([]byte(v.val), []byte(rval)), nil
}

type UUID struct {
	val uuid.UUID
}

func NewUUID(val uuid.UUID) *UUID {
	return &UUID{val: val}
}

func (v *UUID) Type() SQLValueType {
	return UUIDType
}

func (v *UUID) IsNull() bool {
	return false
}

func (v *UUID) String() string {
	return v.val.String()
}

func (v *UUID) inferType(cols map[string]ColDescriptor, params map[string]SQLValueType, implicitTable string) (SQLValueType, error) {
	return UUIDType, nil
}

func (v *UUID) requiresType(t SQLValueType, cols map[string]ColDescriptor, params map[string]SQLValueType, implicitTable string) error {
	if t != UUIDType {
		return fmt.Errorf("%w: %v can not be interpreted as type %v", ErrInvalidTypes, UUIDType, t)
	}

	return nil
}

func (v *UUID) selectors() []Selector {
	return nil
}

func (v *UUID) substitute(params map[string]interface{}) (ValueExp, error) {
	return v, nil
}

func (v *UUID) reduce(tx *SQLTx, row *Row, implicitTable string) (TypedValue, error) {
	return v, nil
}

func (v *UUID) reduceSelectors(row *Row, implicitTable string) ValueExp {
	return v
}

func (v *UUID) isConstant() bool {
	return true
}

func (v *UUID) selectorRanges(table *Table, asTable string, params map[string]interface{}, rangesByColID map[uint32]*typedValueRange) error {
	return nil
}

func (v *UUID) RawValue() interface{} {
	return v.val
}

func (v *UUID) Compare(val TypedValue) (int, error) {
	if val.IsNull() {
		return 1, nil
	}

	if val.Type() != UUIDType {
		return 0, ErrNotComparableValues
	}

	rval := val.RawValue().(uuid.UUID)

	return bytes.Compare(v.val[:], rval[:]), nil
}

type Bool struct {
	val bool
}

func NewBool(val bool) *Bool {
	return &Bool{val: val}
}

func (v *Bool) Type() SQLValueType {
	return BooleanType
}

func (v *Bool) IsNull() bool {
	return false
}

func (v *Bool) String() string {
	return strconv.FormatBool(v.val)
}

func (v *Bool) inferType(cols map[string]ColDescriptor, params map[string]SQLValueType, implicitTable string) (SQLValueType, error) {
	return BooleanType, nil
}

func (v *Bool) requiresType(t SQLValueType, cols map[string]ColDescriptor, params map[string]SQLValueType, implicitTable string) error {
	if t != BooleanType && t != JSONType {
		return fmt.Errorf("%w: %v can not be interpreted as type %v", ErrInvalidTypes, BooleanType, t)
	}
	return nil
}

func (v *Bool) selectors() []Selector {
	return nil
}

func (v *Bool) substitute(params map[string]interface{}) (ValueExp, error) {
	return v, nil
}

func (v *Bool) reduce(tx *SQLTx, row *Row, implicitTable string) (TypedValue, error) {
	return v, nil
}

func (v *Bool) reduceSelectors(row *Row, implicitTable string) ValueExp {
	return v
}

func (v *Bool) isConstant() bool {
	return true
}

func (v *Bool) selectorRanges(table *Table, asTable string, params map[string]interface{}, rangesByColID map[uint32]*typedValueRange) error {
	return nil
}

func (v *Bool) RawValue() interface{} {
	return v.val
}

func (v *Bool) Compare(val TypedValue) (int, error) {
	if val.IsNull() {
		return 1, nil
	}

	if val.Type() == JSONType {
		res, err := val.Compare(v)
		return -res, err
	}

	if val.Type() != BooleanType {
		return 0, ErrNotComparableValues
	}

	rval := val.RawValue().(bool)

	if v.val == rval {
		return 0, nil
	}

	if v.val {
		return 1, nil
	}

	return -1, nil
}

type Blob struct {
	val []byte
}

func NewBlob(val []byte) *Blob {
	return &Blob{val: val}
}

func (v *Blob) Type() SQLValueType {
	return BLOBType
}

func (v *Blob) IsNull() bool {
	return false
}

func (v *Blob) String() string {
	return hex.EncodeToString(v.val)
}

func (v *Blob) inferType(cols map[string]ColDescriptor, params map[string]SQLValueType, implicitTable string) (SQLValueType, error) {
	return BLOBType, nil
}

func (v *Blob) requiresType(t SQLValueType, cols map[string]ColDescriptor, params map[string]SQLValueType, implicitTable string) error {
	if t != BLOBType {
		return fmt.Errorf("%w: %v can not be interpreted as type %v", ErrInvalidTypes, BLOBType, t)
	}

	return nil
}

func (v *Blob) selectors() []Selector {
	return nil
}

func (v *Blob) substitute(params map[string]interface{}) (ValueExp, error) {
	return v, nil
}

func (v *Blob) reduce(tx *SQLTx, row *Row, implicitTable string) (TypedValue, error) {
	return v, nil
}

func (v *Blob) reduceSelectors(row *Row, implicitTable string) ValueExp {
	return v
}

func (v *Blob) isConstant() bool {
	return true
}

func (v *Blob) selectorRanges(table *Table, asTable string, params map[string]interface{}, rangesByColID map[uint32]*typedValueRange) error {
	return nil
}

func (v *Blob) RawValue() interface{} {
	return v.val
}

func (v *Blob) Compare(val TypedValue) (int, error) {
	if val.IsNull() {
		return 1, nil
	}

	if val.Type() != BLOBType {
		return 0, ErrNotComparableValues
	}

	rval := val.RawValue().([]byte)

	return bytes.Compare(v.val, rval), nil
}

type Float64 struct {
	val float64
}

func NewFloat64(val float64) *Float64 {
	return &Float64{val: val}
}

func (v *Float64) Type() SQLValueType {
	return Float64Type
}

func (v *Float64) IsNull() bool {
	return false
}

func (v *Float64) String() string {
	return strconv.FormatFloat(float64(v.val), 'f', -1, 64)
}

func (v *Float64) inferType(cols map[string]ColDescriptor, params map[string]SQLValueType, implicitTable string) (SQLValueType, error) {
	return Float64Type, nil
}

func (v *Float64) requiresType(t SQLValueType, cols map[string]ColDescriptor, params map[string]SQLValueType, implicitTable string) error {
	if t != Float64Type && t != JSONType {
		return fmt.Errorf("%w: %v can not be interpreted as type %v", ErrInvalidTypes, Float64Type, t)
	}
	return nil
}

func (v *Float64) selectors() []Selector {
	return nil
}

func (v *Float64) substitute(params map[string]interface{}) (ValueExp, error) {
	return v, nil
}

func (v *Float64) reduce(tx *SQLTx, row *Row, implicitTable string) (TypedValue, error) {
	return v, nil
}

func (v *Float64) reduceSelectors(row *Row, implicitTable string) ValueExp {
	return v
}

func (v *Float64) isConstant() bool {
	return true
}

func (v *Float64) selectorRanges(table *Table, asTable string, params map[string]interface{}, rangesByColID map[uint32]*typedValueRange) error {
	return nil
}

func (v *Float64) RawValue() interface{} {
	return v.val
}

func (v *Float64) Compare(val TypedValue) (int, error) {
	if val.Type() == JSONType {
		res, err := val.Compare(v)
		return -res, err
	}

	convVal, err := mayApplyImplicitConversion(val.RawValue(), Float64Type)
	if err != nil {
		return 0, err
	}

	if convVal == nil {
		return 1, nil
	}

	rval, ok := convVal.(float64)
	if !ok {
		return 0, ErrNotComparableValues
	}

	if v.val == rval {
		return 0, nil
	}

	if v.val > rval {
		return 1, nil
	}

	return -1, nil
}

// WindowFnExp represents a window function expression: fn(...) OVER (PARTITION BY ... ORDER BY ...)
type WindowFnExp struct {
	fnName      string
	params      []ValueExp
	partitionBy []ValueExp
	orderBy     []*OrdExp
	alias       string // column alias for the result
}

func (v *WindowFnExp) inferType(cols map[string]ColDescriptor, params map[string]SQLValueType, implicitTable string) (SQLValueType, error) {
	return v.resultType(), nil
}

func (v *WindowFnExp) requiresType(t SQLValueType, cols map[string]ColDescriptor, params map[string]SQLValueType, implicitTable string) error {
	return nil
}

func (v *WindowFnExp) substitute(params map[string]interface{}) (ValueExp, error) {
	return v, nil
}

func (v *WindowFnExp) reduce(tx *SQLTx, row *Row, implicitTable string) (TypedValue, error) {
	// Window function values are computed by windowRowReader, not during reduce.
	// By the time reduce is called, the value should already be in the row.
	sel := v.selectorName()
	if val, ok := row.ValuesBySelector[sel]; ok {
		return val, nil
	}
	return NewNull(v.resultType()), nil
}

func (v *WindowFnExp) selectors() []Selector {
	return nil
}

func (v *WindowFnExp) reduceSelectors(row *Row, implicitTable string) ValueExp {
	return v
}

func (v *WindowFnExp) isConstant() bool {
	return false
}

func (v *WindowFnExp) selectorRanges(table *Table, asTable string, params map[string]interface{}, rangesByColID map[uint32]*typedValueRange) error {
	return nil
}

func (v *WindowFnExp) String() string {
	return v.selectorName()
}

func (v *WindowFnExp) selectorName() string {
	if v.alias != "" {
		return v.alias
	}
	return v.fnName + "_over"
}

func (v *WindowFnExp) resultType() SQLValueType {
	switch v.fnName {
	case "ROW_NUMBER", "RANK", "DENSE_RANK", "COUNT", "NTILE":
		return IntegerType
	case "SUM", "AVG":
		return Float64Type
	default:
		return AnyType
	}
}

type FnCall struct {
	fn     string
	params []ValueExp
}

func (v *FnCall) inferType(cols map[string]ColDescriptor, params map[string]SQLValueType, implicitTable string) (SQLValueType, error) {
	fn, err := v.resolveFunc()
	if err != nil {
		return AnyType, nil
	}
	return fn.InferType(cols, params, implicitTable)
}

func (v *FnCall) requiresType(t SQLValueType, cols map[string]ColDescriptor, params map[string]SQLValueType, implicitTable string) error {
	fn, err := v.resolveFunc()
	if err != nil {
		return err
	}
	return fn.RequiresType(t, cols, params, implicitTable)
}

func (v *FnCall) selectors() []Selector {
	selectors := make([]Selector, 0)
	for _, param := range v.params {
		selectors = append(selectors, param.selectors()...)
	}
	return selectors
}

func (v *FnCall) substitute(params map[string]interface{}) (val ValueExp, err error) {
	ps := make([]ValueExp, len(v.params))
	for i, p := range v.params {
		ps[i], err = p.substitute(params)
		if err != nil {
			return nil, err
		}
	}

	return &FnCall{
		fn:     v.fn,
		params: ps,
	}, nil
}

func (v *FnCall) reduce(tx *SQLTx, row *Row, implicitTable string) (TypedValue, error) {
	fn, err := v.resolveFunc()
	if err != nil {
		return nil, err
	}

	fnInputs, err := v.reduceParams(tx, row, implicitTable)
	if err != nil {
		return nil, err
	}
	return fn.Apply(tx, fnInputs)
}

func (v *FnCall) reduceParams(tx *SQLTx, row *Row, implicitTable string) ([]TypedValue, error) {
	var values []TypedValue
	if len(v.params) > 0 {
		values = make([]TypedValue, len(v.params))
		for i, p := range v.params {
			v, err := p.reduce(tx, row, implicitTable)
			if err != nil {
				return nil, err
			}
			values[i] = v
		}
	}
	return values, nil
}

func (v *FnCall) resolveFunc() (Function, error) {
	fn, exists := builtinFunctions[strings.ToUpper(v.fn)]
	if !exists {
		return nil, fmt.Errorf("%w: unknown function %s", ErrIllegalArguments, v.fn)
	}
	return fn, nil
}

func (v *FnCall) reduceSelectors(row *Row, implicitTable string) ValueExp {
	return v
}

func (v *FnCall) isConstant() bool {
	return false
}

func (v *FnCall) selectorRanges(table *Table, asTable string, params map[string]interface{}, rangesByColID map[uint32]*typedValueRange) error {
	return nil
}

func (v *FnCall) String() string {
	params := make([]string, len(v.params))
	for i, p := range v.params {
		params[i] = p.String()
	}
	return v.fn + "(" + strings.Join(params, ",") + ")"
}

type Cast struct {
	val ValueExp
	t   SQLValueType
}

func (c *Cast) inferType(cols map[string]ColDescriptor, params map[string]SQLValueType, implicitTable string) (SQLValueType, error) {
	_, err := c.val.inferType(cols, params, implicitTable)
	if err != nil {
		return AnyType, err
	}

	// val type may be restricted by compatible conversions, but multiple types may be compatible...

	return c.t, nil
}

func (c *Cast) requiresType(t SQLValueType, cols map[string]ColDescriptor, params map[string]SQLValueType, implicitTable string) error {
	if c.t != t {
		return fmt.Errorf("%w: can not use value cast to %s as %s", ErrInvalidTypes, c.t, t)
	}

	return nil
}

func (c *Cast) substitute(params map[string]interface{}) (ValueExp, error) {
	val, err := c.val.substitute(params)
	if err != nil {
		return nil, err
	}
	c.val = val
	return c, nil
}

func (c *Cast) reduce(tx *SQLTx, row *Row, implicitTable string) (TypedValue, error) {
	val, err := c.val.reduce(tx, row, implicitTable)
	if err != nil {
		return nil, err
	}

	conv, err := getConverter(val.Type(), c.t)
	if conv == nil {
		return nil, err
	}

	return conv(val)
}

func (v *Cast) selectors() []Selector {
	return v.val.selectors()
}

func (c *Cast) reduceSelectors(row *Row, implicitTable string) ValueExp {
	return &Cast{
		val: c.val.reduceSelectors(row, implicitTable),
		t:   c.t,
	}
}

func (c *Cast) isConstant() bool {
	return c.val.isConstant()
}

func (c *Cast) selectorRanges(table *Table, asTable string, params map[string]interface{}, rangesByColID map[uint32]*typedValueRange) error {
	return nil
}

func (c *Cast) String() string {
	return fmt.Sprintf("CAST (%s AS %s)", c.val.String(), c.t)
}

type Param struct {
	id  string
	pos int
}

func (v *Param) inferType(cols map[string]ColDescriptor, params map[string]SQLValueType, implicitTable string) (SQLValueType, error) {
	t, ok := params[v.id]
	if !ok {
		params[v.id] = AnyType
		return AnyType, nil
	}

	return t, nil
}

func (v *Param) requiresType(t SQLValueType, cols map[string]ColDescriptor, params map[string]SQLValueType, implicitTable string) error {
	currT, ok := params[v.id]
	if ok && currT != t && currT != AnyType {
		return ErrInferredMultipleTypes
	}

	params[v.id] = t

	return nil
}

func (p *Param) substitute(params map[string]interface{}) (ValueExp, error) {
	val, ok := params[p.id]
	if !ok {
		return nil, fmt.Errorf("%w(%s)", ErrMissingParameter, p.id)
	}

	if val == nil {
		return &NullValue{t: AnyType}, nil
	}

	switch v := val.(type) {
	case bool:
		{
			return &Bool{val: v}, nil
		}
	case string:
		{
			return &Varchar{val: v}, nil
		}
	case int:
		{
			return &Integer{val: int64(v)}, nil
		}
	case uint:
		{
			return &Integer{val: int64(v)}, nil
		}
	case uint64:
		{
			return &Integer{val: int64(v)}, nil
		}
	case int64:
		{
			return &Integer{val: v}, nil
		}
	case []byte:
		{
			return &Blob{val: v}, nil
		}
	case time.Time:
		{
			return &Timestamp{val: v.Truncate(time.Microsecond).UTC()}, nil
		}
	case float64:
		{
			return &Float64{val: v}, nil
		}
	}
	return nil, ErrUnsupportedParameter
}

func (p *Param) reduce(tx *SQLTx, row *Row, implicitTable string) (TypedValue, error) {
	return nil, ErrUnexpected
}

func (p *Param) selectors() []Selector {
	return nil
}

func (p *Param) reduceSelectors(row *Row, implicitTable string) ValueExp {
	return p
}

func (p *Param) isConstant() bool {
	return true
}

func (v *Param) selectorRanges(table *Table, asTable string, params map[string]interface{}, rangesByColID map[uint32]*typedValueRange) error {
	return nil
}

func (v *Param) String() string {
	return "@" + v.id
}

type whenThenClause struct {
	when, then ValueExp
}

type CaseWhenExp struct {
	exp      ValueExp
	whenThen []whenThenClause
	elseExp  ValueExp
}

func (ce *CaseWhenExp) inferType(cols map[string]ColDescriptor, params map[string]SQLValueType, implicitTable string) (SQLValueType, error) {
	checkType := func(e ValueExp, expectedType SQLValueType) (string, error) {
		t, err := e.inferType(cols, params, implicitTable)
		if err != nil {
			return "", err
		}

		if expectedType == AnyType {
			return t, nil
		}

		if t != expectedType {
			if (t == Float64Type && expectedType == IntegerType) ||
				(t == IntegerType && expectedType == Float64Type) {
				return Float64Type, nil
			}
			return "", fmt.Errorf("%w: CASE types %s and %s cannot be matched", ErrInferredMultipleTypes, expectedType, t)
		}
		return t, nil
	}

	searchType := BooleanType
	inferredResType := AnyType
	if ce.exp != nil {
		t, err := ce.exp.inferType(cols, params, implicitTable)
		if err != nil {
			return "", err
		}
		searchType = t
	}

	for _, e := range ce.whenThen {
		whenType, err := e.when.inferType(cols, params, implicitTable)
		if err != nil {
			return "", err
		}

		if whenType != searchType {
			return "", fmt.Errorf("%w: argument of CASE/WHEN must be of type %s, not type %s", ErrInvalidTypes, searchType, whenType)
		}

		t, err := checkType(e.then, inferredResType)
		if err != nil {
			return "", err
		}
		inferredResType = t
	}

	if ce.elseExp != nil {
		return checkType(ce.elseExp, inferredResType)
	}
	return inferredResType, nil
}

func (ce *CaseWhenExp) requiresType(t SQLValueType, cols map[string]ColDescriptor, params map[string]SQLValueType, implicitTable string) error {
	inferredType, err := ce.inferType(cols, params, implicitTable)
	if err != nil {
		return err
	}

	if inferredType != t {
		return fmt.Errorf("%w: expected type %s but %s found instead", ErrInvalidTypes, t, inferredType)
	}
	return nil
}

func (ce *CaseWhenExp) substitute(params map[string]interface{}) (ValueExp, error) {
	var exp ValueExp
	if ce.exp != nil {
		e, err := ce.exp.substitute(params)
		if err != nil {
			return nil, err
		}
		exp = e
	}

	whenThen := make([]whenThenClause, len(ce.whenThen))
	for i, wt := range ce.whenThen {
		whenValue, err := wt.when.substitute(params)
		if err != nil {
			return nil, err
		}
		whenThen[i].when = whenValue

		thenValue, err := wt.then.substitute(params)
		if err != nil {
			return nil, err
		}
		whenThen[i].then = thenValue
	}

	if ce.elseExp == nil {
		return &CaseWhenExp{
			exp:      exp,
			whenThen: whenThen,
		}, nil
	}

	elseValue, err := ce.elseExp.substitute(params)
	if err != nil {
		return nil, err
	}
	return &CaseWhenExp{
		exp:      exp,
		whenThen: whenThen,
		elseExp:  elseValue,
	}, nil
}

func (ce *CaseWhenExp) selectors() []Selector {
	selectors := make([]Selector, 0)
	if ce.exp != nil {
		selectors = append(selectors, ce.exp.selectors()...)
	}

	for _, wh := range ce.whenThen {
		selectors = append(selectors, wh.when.selectors()...)
		selectors = append(selectors, wh.then.selectors()...)
	}

	if ce.elseExp == nil {
		return selectors
	}
	return append(selectors, ce.elseExp.selectors()...)
}

func (ce *CaseWhenExp) reduce(tx *SQLTx, row *Row, implicitTable string) (TypedValue, error) {
	var searchValue TypedValue
	if ce.exp != nil {
		v, err := ce.exp.reduce(tx, row, implicitTable)
		if err != nil {
			return nil, err
		}
		searchValue = v
	} else {
		searchValue = &Bool{val: true}
	}

	for _, wt := range ce.whenThen {
		v, err := wt.when.reduce(tx, row, implicitTable)
		if err != nil {
			return nil, err
		}

		if v.Type() != searchValue.Type() {
			return nil, fmt.Errorf("%w: argument of CASE/WHEN must be type %s, not type %s", ErrInvalidTypes, v.Type(), searchValue.Type())
		}

		res, err := v.Compare(searchValue)
		if err != nil {
			return nil, err
		}
		if res == 0 {
			return wt.then.reduce(tx, row, implicitTable)
		}
	}

	if ce.elseExp == nil {
		return NewNull(AnyType), nil
	}
	return ce.elseExp.reduce(tx, row, implicitTable)
}

func (ce *CaseWhenExp) reduceSelectors(row *Row, implicitTable string) ValueExp {
	whenThen := make([]whenThenClause, len(ce.whenThen))
	for i, wt := range ce.whenThen {
		whenValue := wt.when.reduceSelectors(row, implicitTable)
		whenThen[i].when = whenValue

		thenValue := wt.then.reduceSelectors(row, implicitTable)
		whenThen[i].then = thenValue
	}

	if ce.elseExp == nil {
		return &CaseWhenExp{
			whenThen: whenThen,
		}
	}

	return &CaseWhenExp{
		whenThen: whenThen,
		elseExp:  ce.elseExp.reduceSelectors(row, implicitTable),
	}
}

func (ce *CaseWhenExp) isConstant() bool {
	return false
}

func (ce *CaseWhenExp) selectorRanges(table *Table, asTable string, params map[string]interface{}, rangesByColID map[uint32]*typedValueRange) error {
	return nil
}

func (ce *CaseWhenExp) String() string {
	var sb strings.Builder
	for _, wh := range ce.whenThen {
		sb.WriteString(fmt.Sprintf("WHEN %s THEN %s ", wh.when.String(), wh.then.String()))
	}

	if ce.elseExp != nil {
		sb.WriteString("ELSE " + ce.elseExp.String() + " ")
	}
	return "CASE " + sb.String() + "END"
}

type Comparison int

const (
	EqualTo Comparison = iota
	LowerThan
	LowerOrEqualTo
	GreaterThan
	GreaterOrEqualTo
)

type DataSource interface {
	SQLStmt
	Resolve(ctx context.Context, tx *SQLTx, params map[string]interface{}, scanSpecs *ScanSpecs) (RowReader, error)
	Alias() string
}

type TargetEntry struct {
	Exp ValueExp
	As  string
}

type SelectStmt struct {
	distinct  bool
	targets   []TargetEntry
	selectors []Selector
	ds        DataSource
	indexOn   []string
	joins     []*JoinSpec
	where     ValueExp
	groupBy   []*ColSelector
	having    ValueExp
	orderBy   []*OrdExp
	limit     ValueExp
	offset    ValueExp
	as        string
}

func NewSelectStmt(
	targets []TargetEntry,
	ds DataSource,
	where ValueExp,
	orderBy []*OrdExp,
	limit ValueExp,
	offset ValueExp,
) *SelectStmt {
	return &SelectStmt{
		targets: targets,
		ds:      ds,
		where:   where,
		orderBy: orderBy,
		limit:   limit,
		offset:  offset,
	}
}

func (stmt *SelectStmt) readOnly() bool {
	return true
}

func (stmt *SelectStmt) requiredPrivileges() []SQLPrivilege {
	return []SQLPrivilege{SQLPrivilegeSelect}
}

func (stmt *SelectStmt) inferParameters(ctx context.Context, tx *SQLTx, params map[string]SQLValueType) error {
	_, err := stmt.execAt(ctx, tx, nil)
	if err != nil {
		return err
	}

	// TODO: (jeroiraz) may be optimized so to resolve the query statement just once
	rowReader, err := stmt.Resolve(ctx, tx, nil, nil)
	if err != nil {
		return err
	}
	defer rowReader.Close()

	return rowReader.InferParameters(ctx, params)
}

func (stmt *SelectStmt) execAt(ctx context.Context, tx *SQLTx, params map[string]interface{}) (*SQLTx, error) {
	if stmt.groupBy == nil && stmt.having != nil {
		return nil, ErrHavingClauseRequiresGroupClause
	}

	if stmt.containsAggregations() || len(stmt.groupBy) > 0 {
		for _, sel := range stmt.targetSelectors() {
			_, isAgg := sel.(*AggColSelector)
			if !isAgg && !stmt.groupByContains(sel) {
				return nil, fmt.Errorf("%s: %w", EncodeSelector(sel.resolve(stmt.Alias())), ErrColumnMustAppearInGroupByOrAggregation)
			}
		}
	}

	if len(stmt.orderBy) > 0 {
		// Resolve ORDER BY aliases (e.g. "ORDER BY total" → actual expression)
		stmt.resolveOrderByAliases()

		for _, col := range stmt.orderBy {
			for _, sel := range col.exp.selectors() {
				_, isAgg := sel.(*AggColSelector)
				if (isAgg && !stmt.selectorAppearsInTargets(sel)) || (!isAgg && len(stmt.groupBy) > 0 && !stmt.groupByContains(sel)) {
					return nil, fmt.Errorf("%s: %w", EncodeSelector(sel.resolve(stmt.Alias())), ErrColumnMustAppearInGroupByOrAggregation)
				}
			}
		}
	}
	return tx, nil
}

func (stmt *SelectStmt) targetSelectors() []Selector {
	if stmt.selectors == nil {
		stmt.selectors = stmt.extractSelectors()
	}
	return stmt.selectors
}

func (stmt *SelectStmt) selectorAppearsInTargets(s Selector) bool {
	encSel := EncodeSelector(s.resolve(stmt.Alias()))

	for _, sel := range stmt.targetSelectors() {
		if EncodeSelector(sel.resolve(stmt.Alias())) == encSel {
			return true
		}
	}
	return false
}

// resolveOrderByAliases replaces ORDER BY expressions that reference
// SELECT aliases with the actual target expressions. This enables
// "SELECT col AS alias ... ORDER BY alias" syntax.
func (stmt *SelectStmt) resolveOrderByAliases() {
	for i, ordExp := range stmt.orderBy {
		// Check if the ORDER BY expression is a simple column selector
		sel := ordExp.AsSelector()
		if sel == nil {
			continue
		}

		colSel, ok := sel.(*ColSelector)
		if !ok {
			continue
		}

		// Check if this selector name matches a target alias
		for _, target := range stmt.targets {
			if target.As != "" && strings.EqualFold(target.As, colSel.col) {
				// Replace the ORDER BY expression with the target expression
				stmt.orderBy[i] = &OrdExp{
					exp:        target.Exp,
					descOrder:  ordExp.descOrder,
					nullsOrder: ordExp.nullsOrder,
				}
				break
			}
		}
	}
}

func (stmt *SelectStmt) groupByContains(sel Selector) bool {
	encSel := EncodeSelector(sel.resolve(stmt.Alias()))

	for _, colSel := range stmt.groupBy {
		if EncodeSelector(colSel.resolve(stmt.Alias())) == encSel {
			return true
		}
	}
	return false
}

func (stmt *SelectStmt) extractGroupByCols() []*AggColSelector {
	cols := make([]*AggColSelector, 0, len(stmt.targets))

	for _, t := range stmt.targets {
		selectors := t.Exp.selectors()
		for _, sel := range selectors {
			aggSel, isAgg := sel.(*AggColSelector)
			if isAgg {
				cols = append(cols, aggSel)
			}
		}
	}
	return cols
}

func (stmt *SelectStmt) extractSelectors() []Selector {
	selectors := make([]Selector, 0, len(stmt.targets))
	for _, t := range stmt.targets {
		selectors = append(selectors, t.Exp.selectors()...)
	}
	return selectors
}

func (stmt *SelectStmt) Resolve(ctx context.Context, tx *SQLTx, params map[string]interface{}, _ *ScanSpecs) (ret RowReader, err error) {
	// resolveOrderByAliases is normally called via execAt→validateQuery, but
	// CTEStmt.Resolve() calls this method directly (bypassing execAt). Calling
	// here ensures "ORDER BY alias" always works regardless of entry point.
	stmt.resolveOrderByAliases()

	scanSpecs, err := stmt.genScanSpecs(tx, params)
	if err != nil {
		return nil, err
	}

	rowReader, err := stmt.ds.Resolve(ctx, tx, params, scanSpecs)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err != nil {
			rowReader.Close()
		}
	}()

	if stmt.joins != nil {
		hasFullOuter := false
		for _, jspec := range stmt.joins {
			if jspec.joinType == FullOuterJoin {
				hasFullOuter = true
				break
			}
		}

		if hasFullOuter {
			// Process joins one at a time when FULL OUTER JOIN is present
			for _, jspec := range stmt.joins {
				if jspec.joinType == FullOuterJoin {
					rightQ := &SelectStmt{ds: jspec.ds, indexOn: jspec.indexOn}
					rightReader, jErr := rightQ.Resolve(ctx, tx, params, nil)
					if jErr != nil {
						return nil, jErr
					}
					fojReader, jErr := newFullOuterJoinRowReader(ctx, rowReader, rightReader, jspec.cond)
					if jErr != nil {
						rightReader.Close()
						return nil, jErr
					}
					rowReader = fojReader
				} else {
					jReader, jErr := newJointRowReader(rowReader, []*JoinSpec{jspec})
					if jErr != nil {
						return nil, jErr
					}
					rowReader = jReader
				}
			}
		} else {
			jointRowReader, jErr := newJointRowReader(rowReader, stmt.joins)
			if jErr != nil {
				return nil, jErr
			}
			rowReader = jointRowReader
		}
	}

	if stmt.where != nil {
		rowReader = newConditionalRowReader(rowReader, stmt.where)
	}

	// Fast-path: SELECT COUNT(*) FROM tbl with no WHERE, JOINs, GROUP BY, or
	// HAVING.  Count index keys directly without decoding any column values.
	if stmt.isBareCountStar() {
		if rawRdr, ok := rowReader.(*rawRowReader); ok {
			rowReader = newCountingRowReader(rawRdr, stmt.targets[0].Exp.(*AggColSelector))
			goto applyOrderBy
		}
	}

	if stmt.containsAggregations() || len(stmt.groupBy) > 0 {
		// Hash aggregate: replace sort(GROUP BY) + stream-group with a single
		// hash-map pass whenever the GROUP BY columns are not already provided
		// in order by the index scan (groupBySortExps != nil).
		//
		// Three sub-cases after hash agg:
		//   a) stmt.orderBy == nil: no ORDER BY — output order is undefined,
		//      which is correct per SQL standard.
		//   b) orderBySortExps != nil: a separate ORDER BY sort is needed;
		//      the applyOrderBy block below adds it.
		//   c) orderBySortExps == nil && stmt.orderBy != nil: rearrangeOrdExps
		//      merged ORDER BY into the GROUP BY sort, which hash agg bypassed.
		//      Re-add the sort here using the original ORDER BY expressions.
		useHashAgg := len(scanSpecs.groupBySortExps) > 0

		if useHashAgg {
			hashGrpRdr, hErr := newHashGroupedRowReader(rowReader, allAggregations(stmt.targets), stmt.extractGroupByCols(), stmt.groupBy)
			if hErr != nil {
				return nil, hErr
			}
			rowReader = hashGrpRdr

			// Case (c): ORDER BY was merged into the GROUP BY sort by
			// rearrangeOrdExps (stmt.orderBy set but orderBySortExps nil).
			// Hash agg bypassed that sort, so re-add it using the merged
			// groupBySortExps key, which covers the ORDER BY prefix and
			// produces more deterministic within-group ordering.
			if stmt.orderBy != nil && len(scanSpecs.orderBySortExps) == 0 {
				sortRdr, sErr := newSortRowReader(rowReader, scanSpecs.groupBySortExps)
				if sErr != nil {
					return nil, sErr
				}
				rowReader = sortRdr
			}
		} else {
			// groupBySortExps == 0: the index already provides GROUP BY order;
			// stream-aggregate directly without an explicit sort.
			var groupedRowReader *groupedRowReader
			groupedRowReader, err = newGroupedRowReader(rowReader, allAggregations(stmt.targets), stmt.extractGroupByCols(), stmt.groupBy)
			if err != nil {
				return nil, err
			}
			rowReader = groupedRowReader
		}

		if stmt.having != nil {
			rowReader = newConditionalRowReader(rowReader, stmt.having)
		}
	}

applyOrderBy:
	if len(scanSpecs.orderBySortExps) > 0 {
		var sortRdr *sortRowReader
		sortRdr, err = newSortRowReader(rowReader, stmt.orderBy)
		if err != nil {
			return nil, err
		}
		// Top-N heap optimisation: when ORDER BY is paired with a small
		// constant LIMIT and no OFFSET, use a bounded heap instead of a
		// full sort so that only N rows are kept in memory.
		if stmt.limit != nil && stmt.offset == nil {
			if lv, lErr := evalExpAsInt(tx, stmt.limit, params); lErr == nil && lv > 0 && lv <= topNSortThreshold {
				sortRdr.topNLimit = lv
			}
		}
		rowReader = sortRdr
	}

	// Detect window functions in targets and wrap with windowRowReader
	var windowFns []*WindowFnExp
	for i, t := range stmt.targets {
		if wfn, ok := t.Exp.(*WindowFnExp); ok {
			if wfn.alias == "" {
				if t.As != "" {
					wfn.alias = t.As
				} else {
					wfn.alias = fmt.Sprintf("%s_%d", wfn.fnName, i)
				}
			}
			windowFns = append(windowFns, wfn)
		}
	}

	if len(windowFns) > 0 {
		maxRows := 0
		if tx != nil && tx.engine != nil {
			maxRows = tx.engine.maxWindowRows
		}
		winReader, wErr := newWindowRowReader(ctx, rowReader, windowFns, maxRows)
		if wErr != nil {
			return nil, wErr
		}
		rowReader = winReader
	}

	projectedRowReader, err := newProjectedRowReader(ctx, rowReader, stmt.as, stmt.targets)
	if err != nil {
		return nil, err
	}
	rowReader = projectedRowReader

	if stmt.distinct {
		var distinctRowReader *distinctRowReader
		distinctRowReader, err = newDistinctRowReader(ctx, rowReader)
		if err != nil {
			return nil, err
		}
		rowReader = distinctRowReader
	}

	if stmt.offset != nil {
		var offset int
		offset, err = evalExpAsInt(tx, stmt.offset, params)
		if err != nil {
			return nil, fmt.Errorf("%w: invalid offset", err)
		}

		rowReader = newOffsetRowReader(rowReader, offset)
	}

	if stmt.limit != nil {
		var limit int
		limit, err = evalExpAsInt(tx, stmt.limit, params)
		if err != nil {
			return nil, fmt.Errorf("%w: invalid limit", err)
		}

		if limit < 0 {
			return nil, fmt.Errorf("%w: invalid limit", ErrIllegalArguments)
		}

		if limit > 0 {
			rowReader = newLimitRowReader(rowReader, limit)
		}
	}
	return rowReader, nil
}

func (stmt *SelectStmt) rearrangeOrdExps(groupByCols, orderByExps []*OrdExp) ([]*OrdExp, []*OrdExp) {
	if len(groupByCols) > 0 && len(orderByExps) > 0 && !ordExpsHaveAggregations(orderByExps) {
		if ordExpsHasPrefix(orderByExps, groupByCols, stmt.Alias()) {
			return orderByExps, nil
		}

		if ordExpsHasPrefix(groupByCols, orderByExps, stmt.Alias()) {
			for i := range orderByExps {
				groupByCols[i].descOrder = orderByExps[i].descOrder
			}
			return groupByCols, nil
		}
	}
	return groupByCols, orderByExps
}

func ordExpsHasPrefix(cols, prefix []*OrdExp, table string) bool {
	if len(prefix) > len(cols) {
		return false
	}

	for i := range prefix {
		ls := prefix[i].AsSelector()
		rs := cols[i].AsSelector()

		if ls == nil || rs == nil {
			return false
		}

		if EncodeSelector(ls.resolve(table)) != EncodeSelector(rs.resolve(table)) {
			return false
		}
	}
	return true
}

func (stmt *SelectStmt) groupByOrdExps() []*OrdExp {
	groupByCols := stmt.groupBy

	ordExps := make([]*OrdExp, 0, len(groupByCols))
	for _, col := range groupByCols {
		ordExps = append(ordExps, &OrdExp{exp: col})
	}
	return ordExps
}

func ordExpsHaveAggregations(exps []*OrdExp) bool {
	for _, e := range exps {
		if _, isAgg := e.exp.(*AggColSelector); isAgg {
			return true
		}
	}
	return false
}

func (stmt *SelectStmt) containsAggregations() bool {
	for _, sel := range stmt.targetSelectors() {
		_, isAgg := sel.(*AggColSelector)
		if isAgg {
			return true
		}
	}
	return false
}

// expContainsSubquery reports whether exp contains any correlated subquery
// expression (EXISTS, IN-subquery).  Those expressions reference outer-scope
// columns through their inner SELECT rather than through the Selector graph,
// so selectors() cannot enumerate them.  When present, projection pushdown
// must be disabled for safety.
func expContainsSubquery(exp ValueExp) bool {
	switch e := exp.(type) {
	case *ExistsBoolExp:
		return true
	case *InSubQueryExp:
		return true
	case *BinBoolExp:
		return expContainsSubquery(e.left) || expContainsSubquery(e.right)
	case *NotBoolExp:
		return expContainsSubquery(e.exp)
	case *CmpBoolExp:
		return expContainsSubquery(e.left) || expContainsSubquery(e.right)
	case *CaseWhenExp:
		if e.exp != nil && expContainsSubquery(e.exp) {
			return true
		}
		for _, wt := range e.whenThen {
			if expContainsSubquery(wt.when) || expContainsSubquery(wt.then) {
				return true
			}
		}
		if e.elseExp != nil {
			return expContainsSubquery(e.elseExp)
		}
		return false
	}
	return false
}

// collectNeededColIDs walks all expressions in the statement and returns the
// set of column IDs from table that must be decoded.  The second return value
// is true when all columns are required (wildcard SELECT or unresolvable ref),
// in which case projection pushdown must not be applied.
//
// Conservative by design: any doubt → returns (nil, true) → decode all.
func (stmt *SelectStmt) collectNeededColIDs(table *Table, tableAlias string) (map[uint32]bool, bool) {
	// SELECT * is represented as an empty targets slice; the expansion to
	// individual columns happens later in newProjectedRowReader.  Skip pushdown.
	if len(stmt.targets) == 0 {
		return nil, true
	}

	// Window functions do not expose inner column references through selectors().
	// Disable pushdown for any query that has them.
	for _, t := range stmt.targets {
		if _, isWin := t.Exp.(*WindowFnExp); isWin {
			return nil, true
		}
	}

	// Correlated subqueries reference outer columns through the inner SELECT,
	// which is invisible to selectors().  Disable pushdown when present.
	for _, exp := range []ValueExp{stmt.where, stmt.having} {
		if exp != nil && expContainsSubquery(exp) {
			return nil, true
		}
	}

	// Gather all ValueExp nodes whose column references must be decoded.
	allExps := make([]ValueExp, 0, len(stmt.targets)+4)
	for _, t := range stmt.targets {
		allExps = append(allExps, t.Exp)
	}
	if stmt.where != nil {
		allExps = append(allExps, stmt.where)
	}
	for _, gb := range stmt.groupBy {
		allExps = append(allExps, gb)
	}
	for _, ob := range stmt.orderBy {
		allExps = append(allExps, ob.exp)
	}
	if stmt.having != nil {
		allExps = append(allExps, stmt.having)
	}
	for _, j := range stmt.joins {
		if j.cond != nil {
			allExps = append(allExps, j.cond)
		}
	}

	needed := make(map[uint32]bool)
	for _, exp := range allExps {
		for _, sel := range exp.selectors() {
			var colName, colTable string
			switch s := sel.(type) {
			case *ColSelector:
				if s.col == "*" {
					return nil, true // SELECT * — decode everything
				}
				colName, colTable = s.col, s.table
			case *AggColSelector:
				if s.col == "*" {
					continue // COUNT(*) needs no specific column
				}
				colName, colTable = s.col, s.table
			default:
				return nil, true // unknown selector type, be safe
			}
			if colTable != "" && colTable != tableAlias {
				continue // column belongs to a joined table, not this scan
			}
			col, err := table.GetColumnByName(colName)
			if err != nil {
				return nil, true // unresolvable, decode all
			}
			needed[col.id] = true
		}
	}
	return needed, false
}

// isBareCountStar reports whether the statement is exactly
// SELECT COUNT(*) FROM tbl with no WHERE, JOINs, GROUP BY, or HAVING.
// Such queries can be answered by counting index keys without decoding values.
func (stmt *SelectStmt) isBareCountStar() bool {
	if len(stmt.targets) != 1 || stmt.where != nil ||
		len(stmt.groupBy) > 0 || stmt.having != nil || len(stmt.joins) > 0 {
		return false
	}
	agg, ok := stmt.targets[0].Exp.(*AggColSelector)
	return ok && agg.aggFn == COUNT && agg.col == "*" && !agg.distinct
}

func evalExpAsInt(tx *SQLTx, exp ValueExp, params map[string]interface{}) (int, error) {
	offset, err := exp.substitute(params)
	if err != nil {
		return 0, err
	}

	texp, err := offset.reduce(tx, nil, "")
	if err != nil {
		return 0, err
	}

	convVal, err := mayApplyImplicitConversion(texp.RawValue(), IntegerType)
	if err != nil {
		return 0, ErrInvalidValue
	}

	num, ok := convVal.(int64)
	if !ok {
		return 0, ErrInvalidValue
	}

	if num > math.MaxInt32 {
		return 0, ErrInvalidValue
	}

	return int(num), nil
}

func (stmt *SelectStmt) Alias() string {
	if stmt.as == "" {
		return stmt.ds.Alias()
	}

	return stmt.as
}

func (stmt *SelectStmt) hasTxMetadata() bool {
	for _, sel := range stmt.targetSelectors() {
		switch s := sel.(type) {
		case *ColSelector:
			if s.col == txMetadataCol {
				return true
			}
		case *JSONSelector:
			if s.ColSelector.col == txMetadataCol {
				return true
			}
		}
	}
	return false
}

func (stmt *SelectStmt) genScanSpecs(tx *SQLTx, params map[string]interface{}) (*ScanSpecs, error) {
	groupByCols, orderByCols := stmt.groupByOrdExps(), stmt.orderBy

	tableRef, isTableRef := stmt.ds.(*tableRef)
	if !isTableRef {
		groupByCols, orderByCols = stmt.rearrangeOrdExps(groupByCols, orderByCols)

		return &ScanSpecs{
			groupBySortExps: groupByCols,
			orderBySortExps: orderByCols,
		}, nil
	}

	table, err := tableRef.referencedTable(tx)
	if err != nil {
		if tx.engine.tableResolveFor(tableRef.table) != nil {
			return &ScanSpecs{
				groupBySortExps: groupByCols,
				orderBySortExps: orderByCols,
			}, nil
		}
		return nil, err
	}

	rangesByColID := make(map[uint32]*typedValueRange)
	if stmt.where != nil {
		err = stmt.where.selectorRanges(table, tableRef.Alias(), params, rangesByColID)
		if err != nil {
			return nil, err
		}
	}

	preferredIndex, err := stmt.getPreferredIndex(table)
	if err != nil {
		return nil, err
	}

	var sortingIndex *Index
	if preferredIndex == nil {
		sortingIndex = stmt.selectSortingIndex(groupByCols, orderByCols, table, rangesByColID)
	} else {
		sortingIndex = preferredIndex
	}

	if sortingIndex == nil {
		sortingIndex = table.primaryIndex
	}

	if tableRef.history && !sortingIndex.IsPrimary() {
		return nil, fmt.Errorf("%w: historical queries are supported over primary index", ErrIllegalArguments)
	}

	if tableRef.diff && !sortingIndex.IsPrimary() {
		return nil, fmt.Errorf("%w: diff queries are supported over primary index", ErrIllegalArguments)
	}

	// INLJ fallback: when no sort-based index was found and this is not a
	// history/diff scan, look for a secondary index whose leading columns are
	// fully covered by equality ranges. This turns O(N×M) nested-loop join
	// inner scans into O(N+M) index seeks without touching history/diff paths.
	if sortingIndex == table.primaryIndex && !tableRef.history && !tableRef.diff {
		if idx := stmt.selectINLJIndex(table, rangesByColID); idx != nil {
			sortingIndex = idx
		}
	}

	var descOrder bool
	if len(groupByCols) > 0 && sortingIndex.coversOrdCols(groupByCols, rangesByColID) {
		groupByCols = nil
	}

	if len(groupByCols) == 0 && len(orderByCols) > 0 && sortingIndex.coversOrdCols(orderByCols, rangesByColID) {
		descOrder = orderByCols[0].descOrder
		orderByCols = nil
	}

	groupByCols, orderByCols = stmt.rearrangeOrdExps(groupByCols, orderByCols)

	// Projection pushdown: compute the set of column IDs that need to be
	// decoded for this query.  nil means decode all (history/diff/metadata
	// scans always decode everything; wildcard SELECT does too).
	var neededColIDs map[uint32]bool
	if !tableRef.history && !tableRef.diff && !stmt.hasTxMetadata() {
		ids, wildcard := stmt.collectNeededColIDs(table, tableRef.Alias())
		if !wildcard {
			neededColIDs = ids
		}
	}

	return &ScanSpecs{
		Index:             sortingIndex,
		rangesByColID:     rangesByColID,
		IncludeHistory:    tableRef.history,
		IncludeDiff:       tableRef.diff,
		IncludeTxMetadata: stmt.hasTxMetadata(),
		DescOrder:         descOrder,
		groupBySortExps:   groupByCols,
		orderBySortExps:   orderByCols,
		neededColIDs:      neededColIDs,
	}, nil
}

func (stmt *SelectStmt) selectSortingIndex(groupByCols, orderByCols []*OrdExp, table *Table, rangesByColId map[uint32]*typedValueRange) *Index {
	sortCols := groupByCols
	if len(sortCols) == 0 {
		sortCols = orderByCols
	}

	if len(sortCols) == 0 {
		return nil
	}

	for _, idx := range table.indexes {
		if idx.coversOrdCols(sortCols, rangesByColId) {
			return idx
		}
	}
	return nil
}

// selectINLJIndex picks the most selective secondary index for an index-nested-
// loop join (INLJ) fallback. It counts how many consecutive leading columns of
// each non-primary index are covered by point-equality ranges and returns the
// index with the highest count (ties broken by iteration order).
//
// Only called from genScanSpecs when there are no sort columns and the scan is
// not a history/diff read (those require the primary index).
func (stmt *SelectStmt) selectINLJIndex(table *Table, rangesByColID map[uint32]*typedValueRange) *Index {
	var best *Index
	var bestCovered int
	for _, idx := range table.indexes {
		if idx.IsPrimary() {
			continue
		}
		if n := idx.countEqualityCoveredCols(rangesByColID); n > bestCovered {
			best = idx
			bestCovered = n
		}
	}
	return best
}

func (stmt *SelectStmt) getPreferredIndex(table *Table) (*Index, error) {
	if len(stmt.indexOn) == 0 {
		return nil, nil
	}

	cols := make([]*Column, len(stmt.indexOn))
	for i, colName := range stmt.indexOn {
		col, err := table.GetColumnByName(colName)
		if err != nil {
			return nil, err
		}

		cols[i] = col
	}
	return table.GetIndexByName(indexName(table.name, cols))
}

type UnionStmt struct {
	distinct    bool
	left, right DataSource
	as          string
}

func (stmt *UnionStmt) readOnly() bool {
	return true
}

func (stmt *UnionStmt) requiredPrivileges() []SQLPrivilege {
	return []SQLPrivilege{SQLPrivilegeSelect}
}

func (stmt *UnionStmt) inferParameters(ctx context.Context, tx *SQLTx, params map[string]SQLValueType) error {
	err := stmt.left.inferParameters(ctx, tx, params)
	if err != nil {
		return err
	}
	return stmt.right.inferParameters(ctx, tx, params)
}

func (stmt *UnionStmt) execAt(ctx context.Context, tx *SQLTx, params map[string]interface{}) (*SQLTx, error) {
	_, err := stmt.left.execAt(ctx, tx, params)
	if err != nil {
		return tx, err
	}

	return stmt.right.execAt(ctx, tx, params)
}

func (stmt *UnionStmt) resolveUnionAll(ctx context.Context, tx *SQLTx, params map[string]interface{}) (ret RowReader, err error) {
	leftRowReader, err := stmt.left.Resolve(ctx, tx, params, nil)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err != nil {
			leftRowReader.Close()
		}
	}()

	rightRowReader, err := stmt.right.Resolve(ctx, tx, params, nil)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err != nil {
			rightRowReader.Close()
		}
	}()

	rowReader, err := newUnionRowReader(ctx, []RowReader{leftRowReader, rightRowReader})
	if err != nil {
		return nil, err
	}

	if stmt.as != "" {
		rowReader.alias = stmt.as
	}

	return rowReader, nil
}

func (stmt *UnionStmt) Resolve(ctx context.Context, tx *SQLTx, params map[string]interface{}, _ *ScanSpecs) (ret RowReader, err error) {
	rowReader, err := stmt.resolveUnionAll(ctx, tx, params)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err != nil {
			rowReader.Close()
		}
	}()

	if stmt.distinct {
		distinctReader, err := newDistinctRowReader(ctx, rowReader)
		if err != nil {
			return nil, err
		}
		rowReader = distinctReader
	}

	return rowReader, nil
}

func (stmt *UnionStmt) Alias() string {
	return stmt.as
}

// ExceptStmt implements EXCEPT set operation (rows in left but not in right)
type ExceptStmt struct {
	left, right DataSource
	as          string
}

func (stmt *ExceptStmt) readOnly() bool                            { return true }
func (stmt *ExceptStmt) requiredPrivileges() []SQLPrivilege        { return []SQLPrivilege{SQLPrivilegeSelect} }
func (stmt *ExceptStmt) Alias() string                             { return stmt.as }

func (stmt *ExceptStmt) inferParameters(ctx context.Context, tx *SQLTx, params map[string]SQLValueType) error {
	if err := stmt.left.inferParameters(ctx, tx, params); err != nil {
		return err
	}
	return stmt.right.inferParameters(ctx, tx, params)
}

func (stmt *ExceptStmt) execAt(ctx context.Context, tx *SQLTx, params map[string]interface{}) (*SQLTx, error) {
	return tx, nil
}

func (stmt *ExceptStmt) Resolve(ctx context.Context, tx *SQLTx, params map[string]interface{}, _ *ScanSpecs) (ret RowReader, err error) {
	leftReader, err := stmt.left.Resolve(ctx, tx, params, nil)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err != nil {
			leftReader.Close()
		}
	}()

	rightReader, err := stmt.right.Resolve(ctx, tx, params, nil)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err != nil {
			rightReader.Close()
		}
	}()

	return newSetOpRowReader(ctx, leftReader, rightReader, setOpExcept)
}

// IntersectStmt implements INTERSECT set operation (rows in both left and right)
type IntersectStmt struct {
	left, right DataSource
	as          string
}

func (stmt *IntersectStmt) readOnly() bool                            { return true }
func (stmt *IntersectStmt) requiredPrivileges() []SQLPrivilege        { return []SQLPrivilege{SQLPrivilegeSelect} }
func (stmt *IntersectStmt) Alias() string                             { return stmt.as }

func (stmt *IntersectStmt) inferParameters(ctx context.Context, tx *SQLTx, params map[string]SQLValueType) error {
	if err := stmt.left.inferParameters(ctx, tx, params); err != nil {
		return err
	}
	return stmt.right.inferParameters(ctx, tx, params)
}

func (stmt *IntersectStmt) execAt(ctx context.Context, tx *SQLTx, params map[string]interface{}) (*SQLTx, error) {
	return tx, nil
}

func (stmt *IntersectStmt) Resolve(ctx context.Context, tx *SQLTx, params map[string]interface{}, _ *ScanSpecs) (ret RowReader, err error) {
	leftReader, err := stmt.left.Resolve(ctx, tx, params, nil)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err != nil {
			leftReader.Close()
		}
	}()

	rightReader, err := stmt.right.Resolve(ctx, tx, params, nil)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err != nil {
			rightReader.Close()
		}
	}()

	return newSetOpRowReader(ctx, leftReader, rightReader, setOpIntersect)
}

// ExplainStmt returns the query plan as text rows
type ExplainStmt struct {
	query DataSource
}

func (stmt *ExplainStmt) readOnly() bool                     { return true }
func (stmt *ExplainStmt) requiredPrivileges() []SQLPrivilege  { return []SQLPrivilege{SQLPrivilegeSelect} }
func (stmt *ExplainStmt) Alias() string                      { return "" }

func (stmt *ExplainStmt) inferParameters(ctx context.Context, tx *SQLTx, params map[string]SQLValueType) error {
	return stmt.query.inferParameters(ctx, tx, params)
}

func (stmt *ExplainStmt) execAt(ctx context.Context, tx *SQLTx, params map[string]interface{}) (*SQLTx, error) {
	return tx, nil
}

func (stmt *ExplainStmt) Resolve(ctx context.Context, tx *SQLTx, params map[string]interface{}, _ *ScanSpecs) (RowReader, error) {
	// Build a description of the query plan
	lines := stmt.describePlan(stmt.query, 0)

	cols := []ColDescriptor{{Column: "plan", Type: VarcharType}}
	rows := make([][]ValueExp, len(lines))
	for i, line := range lines {
		rows[i] = []ValueExp{NewVarchar(line)}
	}

	return NewValuesRowReader(tx, nil, cols, true, "explain", rows)
}

func (stmt *ExplainStmt) describePlan(ds DataSource, indent int) []string {
	prefix := ""
	for i := 0; i < indent; i++ {
		prefix += "  "
	}

	var lines []string

	switch s := ds.(type) {
	case *SelectStmt:
		scanType := "Seq Scan"
		tableName := ""

		if tr, ok := s.ds.(*tableRef); ok {
			tableName = tr.table
			if tr.as != "" {
				tableName = tr.table + " " + tr.as
			}
		}

		if s.indexOn != nil && len(s.indexOn) > 0 {
			scanType = fmt.Sprintf("Index Scan using (%s)", strings.Join(s.indexOn, ", "))
		}

		line := fmt.Sprintf("%s-> %s on %s", prefix, scanType, tableName)
		lines = append(lines, line)

		if s.where != nil {
			lines = append(lines, fmt.Sprintf("%s  Filter: %s", prefix, s.where.String()))
		}

		if s.joins != nil {
			for _, j := range s.joins {
				joinName := "Inner Join"
				switch j.joinType {
				case LeftJoin:
					joinName = "Left Join"
				case RightJoin:
					joinName = "Right Join"
				case CrossJoin:
					joinName = "Cross Join"
				case FullOuterJoin:
					joinName = "Full Outer Join"
				}
				lines = append(lines, fmt.Sprintf("%s  -> %s", prefix, joinName))
				if j.cond != nil {
					lines = append(lines, fmt.Sprintf("%s    Cond: %s", prefix, j.cond.String()))
				}
				lines = append(lines, stmt.describePlan(j.ds, indent+2)...)
			}
		}

		if s.groupBy != nil {
			cols := make([]string, len(s.groupBy))
			for i, g := range s.groupBy {
				cols[i] = g.String()
			}
			lines = append(lines, fmt.Sprintf("%s  Group By: %s", prefix, strings.Join(cols, ", ")))
		}

		if s.orderBy != nil {
			cols := make([]string, len(s.orderBy))
			for i, o := range s.orderBy {
				dir := "ASC"
				if o.descOrder {
					dir = "DESC"
				}
				cols[i] = fmt.Sprintf("%s %s", o.exp.String(), dir)
			}
			lines = append(lines, fmt.Sprintf("%s  Order By: %s", prefix, strings.Join(cols, ", ")))
		}

		if s.limit != nil {
			lines = append(lines, fmt.Sprintf("%s  Limit: %s", prefix, s.limit.String()))
		}

	case *UnionStmt:
		lines = append(lines, fmt.Sprintf("%s-> Union", prefix))
		lines = append(lines, stmt.describePlan(s.left, indent+1)...)
		lines = append(lines, stmt.describePlan(s.right, indent+1)...)

	case *ExceptStmt:
		lines = append(lines, fmt.Sprintf("%s-> Except", prefix))
		lines = append(lines, stmt.describePlan(s.left, indent+1)...)
		lines = append(lines, stmt.describePlan(s.right, indent+1)...)

	case *IntersectStmt:
		lines = append(lines, fmt.Sprintf("%s-> Intersect", prefix))
		lines = append(lines, stmt.describePlan(s.left, indent+1)...)
		lines = append(lines, stmt.describePlan(s.right, indent+1)...)

	case *CTEStmt:
		lines = append(lines, fmt.Sprintf("%s-> CTE", prefix))
		for _, cte := range s.ctes {
			lines = append(lines, fmt.Sprintf("%s  CTE %s:", prefix, cte.name))
			lines = append(lines, stmt.describePlan(cte.query, indent+2)...)
		}
		lines = append(lines, stmt.describePlan(s.query, indent+1)...)

	case *tableRef:
		lines = append(lines, fmt.Sprintf("%s-> Seq Scan on %s", prefix, s.table))

	default:
		lines = append(lines, fmt.Sprintf("%s-> Scan", prefix))
	}

	return lines
}

// CreateSequenceStmt creates a named sequence for auto-incrementing values
type CreateSequenceStmt struct {
	name       string
	startValue int64
	increment  int64
}

func (stmt *CreateSequenceStmt) readOnly() bool                     { return false }
func (stmt *CreateSequenceStmt) requiredPrivileges() []SQLPrivilege { return []SQLPrivilege{SQLPrivilegeCreate} }

func (stmt *CreateSequenceStmt) inferParameters(ctx context.Context, tx *SQLTx, params map[string]SQLValueType) error {
	return nil
}

func (stmt *CreateSequenceStmt) execAt(ctx context.Context, tx *SQLTx, params map[string]interface{}) (*SQLTx, error) {
	tx.engine.CreateSequence(stmt.name, stmt.startValue, stmt.increment)

	seq := tx.engine.sequences[stmt.name]
	if err := persistSequence(tx, seq); err != nil {
		return nil, err
	}

	return tx, nil
}

// DropSequenceStmt removes a named sequence
type DropSequenceStmt struct {
	name     string
	ifExists bool
}

func (stmt *DropSequenceStmt) readOnly() bool                     { return false }
func (stmt *DropSequenceStmt) requiredPrivileges() []SQLPrivilege { return []SQLPrivilege{SQLPrivilegeDrop} }

func (stmt *DropSequenceStmt) inferParameters(ctx context.Context, tx *SQLTx, params map[string]SQLValueType) error {
	return nil
}

func (stmt *DropSequenceStmt) execAt(ctx context.Context, tx *SQLTx, params map[string]interface{}) (*SQLTx, error) {
	if !tx.engine.DropSequence(stmt.name) && !stmt.ifExists {
		return nil, fmt.Errorf("sequence does not exist (%s)", stmt.name)
	}

	if err := deleteSequence(ctx, tx, stmt.name); err != nil && !stmt.ifExists {
		return nil, err
	}

	return tx, nil
}

// ReturningStmt wraps a DML statement (INSERT/UPDATE/DELETE) with a RETURNING clause,
// making it behave as a DataSource that returns the affected rows.
type ReturningStmt struct {
	dml       SQLStmt
	returning []TargetEntry
	tableName string
}

func (stmt *ReturningStmt) readOnly() bool                     { return false }
func (stmt *ReturningStmt) requiredPrivileges() []SQLPrivilege  { return stmt.dml.requiredPrivileges() }
func (stmt *ReturningStmt) Alias() string                      { return "" }

func (stmt *ReturningStmt) inferParameters(ctx context.Context, tx *SQLTx, params map[string]SQLValueType) error {
	return stmt.dml.inferParameters(ctx, tx, params)
}

func (stmt *ReturningStmt) execAt(ctx context.Context, tx *SQLTx, params map[string]interface{}) (*SQLTx, error) {
	// DML execution happens in Resolve to avoid double execution
	// (QueryPreparedStmt calls execAt then Resolve)
	return tx, nil
}

func (stmt *ReturningStmt) Resolve(ctx context.Context, tx *SQLTx, params map[string]interface{}, _ *ScanSpecs) (RowReader, error) {
	// Execute the DML
	_, err := stmt.dml.execAt(ctx, tx, params)
	if err != nil {
		return nil, err
	}

	// Get the captured rows from the DML statement
	var capturedRows []*Row
	switch s := stmt.dml.(type) {
	case *UpsertIntoStmt:
		capturedRows = s.returnedRows
	case *UpdateStmt:
		capturedRows = s.returnedRows
	case *DeleteFromStmt:
		capturedRows = s.returnedRows
	}

	if len(capturedRows) == 0 {
		// Return empty result with correct columns
		table, tErr := stmt.resolveTable(tx)
		if tErr != nil {
			return nil, tErr
		}
		cols := stmt.buildReturnCols(table)
		return NewValuesRowReader(tx, nil, cols, true, stmt.tableName, nil)
	}

	// Build column descriptors from captured rows and returning targets
	table, err := stmt.resolveTable(tx)
	if err != nil {
		return nil, err
	}

	cols := stmt.buildReturnCols(table)

	// Filter captured rows to only include returning columns
	rows := make([][]ValueExp, len(capturedRows))
	for i, r := range capturedRows {
		rowVals := make([]ValueExp, len(cols))
		for j, col := range cols {
			sel := EncodeSelector("", stmt.tableName, col.Column)
			if v, ok := r.ValuesBySelector[sel]; ok {
				rowVals[j] = v.(ValueExp)
			} else {
				rowVals[j] = NewNull(col.Type)
			}
		}
		rows[i] = rowVals
	}

	return NewValuesRowReader(tx, nil, cols, true, stmt.tableName, rows)
}

func (stmt *ReturningStmt) resolveTable(tx *SQLTx) (*Table, error) {
	return tx.catalog.GetTableByName(stmt.tableName)
}

func (stmt *ReturningStmt) buildReturnCols(table *Table) []ColDescriptor {
	// Check for RETURNING * (star)
	if len(stmt.returning) == 1 {
		if _, isStar := stmt.returning[0].Exp.(*ColSelector); isStar {
			sel := stmt.returning[0].Exp.(*ColSelector)
			if sel.col == "*" {
				cols := make([]ColDescriptor, len(table.Cols()))
				for i, c := range table.Cols() {
					cols[i] = ColDescriptor{Column: c.Name(), Type: c.Type()}
				}
				return cols
			}
		}
	}

	cols := make([]ColDescriptor, len(stmt.returning))
	for i, t := range stmt.returning {
		colName := t.As
		if colName == "" {
			if sel, ok := t.Exp.(*ColSelector); ok {
				colName = sel.col
			} else {
				colName = fmt.Sprintf("col%d", i)
			}
		}

		colType := AnyType
		if col, err := table.GetColumnByName(colName); err == nil {
			colType = col.Type()
		}

		cols[i] = ColDescriptor{Column: colName, Type: colType}
	}
	return cols
}

// CTEDef holds a single CTE definition (name + query)
type CTEDef struct {
	name      string
	query     DataSource
	recursive bool
}

// CTEStmt wraps a query with Common Table Expressions (WITH clause)
type CTEStmt struct {
	ctes  []*CTEDef
	query DataSource
}

func (stmt *CTEStmt) readOnly() bool                     { return true }
func (stmt *CTEStmt) requiredPrivileges() []SQLPrivilege  { return []SQLPrivilege{SQLPrivilegeSelect} }
func (stmt *CTEStmt) Alias() string                      { return stmt.query.Alias() }

func (stmt *CTEStmt) inferParameters(ctx context.Context, tx *SQLTx, params map[string]SQLValueType) error {
	for _, cte := range stmt.ctes {
		if err := cte.query.inferParameters(ctx, tx, params); err != nil {
			return err
		}
	}
	return stmt.query.inferParameters(ctx, tx, params)
}

func (stmt *CTEStmt) execAt(ctx context.Context, tx *SQLTx, params map[string]interface{}) (*SQLTx, error) {
	return tx, nil
}

func (stmt *CTEStmt) Resolve(ctx context.Context, tx *SQLTx, params map[string]interface{}, scanSpecs *ScanSpecs) (RowReader, error) {
	// Materialize each CTE and register as a temporary table resolver
	registeredCTEs := make([]string, 0, len(stmt.ctes))

	cleanup := func() {
		for _, name := range registeredCTEs {
			delete(tx.engine.tableResolvers, name)
		}
	}

	for _, cte := range stmt.ctes {
		if cte.recursive {
			cols, rows, err := materializeRecursiveCTE(ctx, tx, cte, params)
			if err != nil {
				cleanup()
				return nil, err
			}
			tx.engine.registerTableResolver(cte.name, &cteResolver{
				name: cte.name,
				cols: cols,
				rows: rows,
			})
			registeredCTEs = append(registeredCTEs, cte.name)
			continue
		}

		cols, rows, err := materializeCTE(ctx, tx, cte, params)
		if err != nil {
			cleanup()
			return nil, err
		}
		tx.engine.registerTableResolver(cte.name, &cteResolver{
			name: cte.name,
			cols: cols,
			rows: rows,
		})
		registeredCTEs = append(registeredCTEs, cte.name)
	}

	// Resolve the main query with CTEs available
	mainReader, err := stmt.query.Resolve(ctx, tx, params, scanSpecs)
	if err != nil {
		cleanup()
		return nil, err
	}

	// Schedule cleanup when the reader is closed
	mainReader.onClose(cleanup)

	return mainReader, nil
}

func materializeCTE(ctx context.Context, tx *SQLTx, cte *CTEDef, params map[string]interface{}) ([]ColDescriptor, [][]ValueExp, error) {
	reader, err := cte.query.Resolve(ctx, tx, params, nil)
	if err != nil {
		return nil, nil, fmt.Errorf("error resolving CTE '%s': %w", cte.name, err)
	}
	defer reader.Close()

	rawCols, err := reader.Columns(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("error resolving CTE '%s': %w", cte.name, err)
	}

	cols := make([]ColDescriptor, len(rawCols))
	for i, c := range rawCols {
		cols[i] = ColDescriptor{Column: c.Column, Type: c.Type}
	}

	var rows [][]ValueExp
	for {
		row, err := reader.Read(ctx)
		if err == ErrNoMoreRows {
			break
		}
		if err != nil {
			return nil, nil, fmt.Errorf("error materializing CTE '%s': %w", cte.name, err)
		}
		rowValues := make([]ValueExp, len(row.ValuesByPosition))
		for i, v := range row.ValuesByPosition {
			rowValues[i] = v.(ValueExp)
		}
		rows = append(rows, rowValues)
	}

	return cols, rows, nil
}

const maxRecursiveCTEIterations = 1000

func materializeRecursiveCTE(ctx context.Context, tx *SQLTx, cte *CTEDef, params map[string]interface{}) ([]ColDescriptor, [][]ValueExp, error) {
	// A recursive CTE query must be a UNION (base UNION ALL recursive)
	unionStmt, isUnion := cte.query.(*UnionStmt)
	if !isUnion {
		// Non-union recursive CTE — just materialize normally
		return materializeCTE(ctx, tx, cte, params)
	}

	// Step 1: Execute the base (non-recursive) term
	baseReader, err := unionStmt.left.Resolve(ctx, tx, params, nil)
	if err != nil {
		return nil, nil, fmt.Errorf("error resolving recursive CTE '%s' base: %w", cte.name, err)
	}

	rawCols, err := baseReader.Columns(ctx)
	if err != nil {
		baseReader.Close()
		return nil, nil, fmt.Errorf("error resolving recursive CTE '%s': %w", cte.name, err)
	}

	cols := make([]ColDescriptor, len(rawCols))
	for i, c := range rawCols {
		cols[i] = ColDescriptor{Column: c.Column, Type: c.Type}
	}

	var allRows [][]ValueExp
	var newRows [][]ValueExp

	for {
		row, err := baseReader.Read(ctx)
		if err == ErrNoMoreRows {
			break
		}
		if err != nil {
			baseReader.Close()
			return nil, nil, fmt.Errorf("error materializing recursive CTE '%s': %w", cte.name, err)
		}
		rowValues := make([]ValueExp, len(row.ValuesByPosition))
		for i, v := range row.ValuesByPosition {
			rowValues[i] = v.(ValueExp)
		}
		allRows = append(allRows, rowValues)
		newRows = append(newRows, rowValues)
	}
	baseReader.Close()

	// Step 2: Iteratively execute the recursive term
	for iteration := 0; iteration < maxRecursiveCTEIterations; iteration++ {
		if len(newRows) == 0 {
			break
		}

		// Register current results as the CTE resolver (so recursive term can reference it)
		tx.engine.registerTableResolver(cte.name, &cteResolver{
			name: cte.name,
			cols: cols,
			rows: newRows, // Only new rows from the previous iteration
		})

		recReader, err := unionStmt.right.Resolve(ctx, tx, params, nil)
		if err != nil {
			return nil, nil, fmt.Errorf("error in recursive CTE '%s' iteration %d: %w", cte.name, iteration, err)
		}

		newRows = nil
		for {
			row, err := recReader.Read(ctx)
			if err == ErrNoMoreRows {
				break
			}
			if err != nil {
				recReader.Close()
				return nil, nil, fmt.Errorf("error in recursive CTE '%s' iteration %d: %w", cte.name, iteration, err)
			}
			rowValues := make([]ValueExp, len(row.ValuesByPosition))
			for i, v := range row.ValuesByPosition {
				rowValues[i] = v.(ValueExp)
			}
			allRows = append(allRows, rowValues)
			newRows = append(newRows, rowValues)
		}
		recReader.Close()
	}

	return cols, allRows, nil
}

// cteResolver implements TableResolver for materialized CTEs
type cteResolver struct {
	name string
	cols []ColDescriptor
	rows [][]ValueExp
}

func (r *cteResolver) Table() string { return r.name }

func (r *cteResolver) Resolve(ctx context.Context, tx *SQLTx, alias string) (RowReader, error) {
	if alias == "" {
		alias = r.name
	}
	return NewValuesRowReader(tx, nil, r.cols, true, alias, r.rows)
}

// CreateViewStmt creates a named view backed by a SELECT query
type CreateViewStmt struct {
	viewName    string
	ifNotExists bool
	query       DataSource
	querySQL    string // raw SQL for persistence
}

func (stmt *CreateViewStmt) readOnly() bool                     { return false }
func (stmt *CreateViewStmt) requiredPrivileges() []SQLPrivilege { return []SQLPrivilege{SQLPrivilegeCreate} }

func (stmt *CreateViewStmt) inferParameters(ctx context.Context, tx *SQLTx, params map[string]SQLValueType) error {
	return nil
}

func (stmt *CreateViewStmt) execAt(ctx context.Context, tx *SQLTx, params map[string]interface{}) (*SQLTx, error) {
	if tx.engine.tableResolveFor(stmt.viewName) != nil {
		if stmt.ifNotExists {
			return tx, nil
		}
		return nil, fmt.Errorf("%w (%s)", ErrTableAlreadyExists, stmt.viewName)
	}

	// Check that the view name doesn't conflict with a real table
	if tx.catalog != nil {
		if _, exists := tx.catalog.tablesByName[stmt.viewName]; exists {
			return nil, fmt.Errorf("%w (%s)", ErrTableAlreadyExists, stmt.viewName)
		}
	}

	tx.engine.registerTableResolver(stmt.viewName, &viewResolver{
		name:  stmt.viewName,
		query: stmt.query,
	})

	// Persist view to catalog storage so it survives restart
	if stmt.querySQL != "" {
		if err := persistView(tx, stmt.viewName, stmt.querySQL); err != nil {
			return nil, fmt.Errorf("failed to persist view %s: %w", stmt.viewName, err)
		}
	}

	return tx, nil
}

// DropViewStmt removes a named view
type DropViewStmt struct {
	viewName string
	ifExists bool
}

func (stmt *DropViewStmt) readOnly() bool                     { return false }
func (stmt *DropViewStmt) requiredPrivileges() []SQLPrivilege { return []SQLPrivilege{SQLPrivilegeCreate} }

func (stmt *DropViewStmt) inferParameters(ctx context.Context, tx *SQLTx, params map[string]SQLValueType) error {
	return nil
}

func (stmt *DropViewStmt) execAt(ctx context.Context, tx *SQLTx, params map[string]interface{}) (*SQLTx, error) {
	if tx.engine.tableResolveFor(stmt.viewName) == nil {
		if stmt.ifExists {
			return tx, nil
		}
		return nil, fmt.Errorf("view does not exist (%s)", stmt.viewName)
	}

	delete(tx.engine.tableResolvers, stmt.viewName)

	// Remove from persistent storage (ignore errors for legacy session-scoped views)
	_ = deleteView(ctx, tx, stmt.viewName)

	return tx, nil
}

// viewResolver implements TableResolver for views
type viewResolver struct {
	name     string
	query    DataSource
	querySQL string // stored for persistence
}

func (r *viewResolver) Table() string { return r.name }

func (r *viewResolver) Resolve(ctx context.Context, tx *SQLTx, alias string) (RowReader, error) {
	// Re-parse from SQL if query is nil (loaded from persistence)
	q := r.query
	if q == nil && r.querySQL != "" {
		stmts, err := ParseSQL(strings.NewReader(r.querySQL))
		if err != nil {
			return nil, fmt.Errorf("error parsing view '%s': %w", r.name, err)
		}
		if len(stmts) != 1 {
			return nil, fmt.Errorf("view '%s' must contain exactly one statement", r.name)
		}
		ds, ok := stmts[0].(DataSource)
		if !ok {
			return nil, fmt.Errorf("view '%s' must be a SELECT statement", r.name)
		}
		q = ds
	}
	if q == nil {
		return nil, fmt.Errorf("view '%s' has no query", r.name)
	}

	reader, err := q.Resolve(ctx, tx, nil, nil)
	if err != nil {
		return nil, fmt.Errorf("error resolving view '%s': %w", r.name, err)
	}
	return reader, nil
}

func NewTableRef(table string, as string) *tableRef {
	return &tableRef{
		table: table,
		as:    as,
	}
}

type tableRef struct {
	table   string
	history bool
	diff    bool
	period  period
	as      string
}

func (ref *tableRef) readOnly() bool {
	return true
}

func (ref *tableRef) requiredPrivileges() []SQLPrivilege {
	return []SQLPrivilege{SQLPrivilegeSelect}
}

type period struct {
	start *openPeriod
	end   *openPeriod
}

type openPeriod struct {
	inclusive bool
	instant   periodInstant
}

type periodInstant struct {
	exp         ValueExp
	instantType instantType
}

type instantType = int

const (
	txInstant instantType = iota
	timeInstant
)

func (i periodInstant) resolve(tx *SQLTx, params map[string]interface{}, asc, inclusive bool) (uint64, error) {
	exp, err := i.exp.substitute(params)
	if err != nil {
		return 0, err
	}

	instantVal, err := exp.reduce(tx, nil, "")
	if err != nil {
		return 0, err
	}

	if i.instantType == txInstant {
		txID, ok := instantVal.RawValue().(int64)
		if !ok {
			return 0, fmt.Errorf("%w: invalid tx range, tx ID must be a positive integer, %s given", ErrIllegalArguments, instantVal.Type())
		}

		if txID <= 0 {
			return 0, fmt.Errorf("%w: invalid tx range, tx ID must be a positive integer, %d given", ErrIllegalArguments, txID)
		}

		if inclusive {
			return uint64(txID), nil
		}

		if asc {
			return uint64(txID + 1), nil
		}

		if txID <= 1 {
			return 0, fmt.Errorf("%w: invalid tx range, tx ID must be greater than 1, %d given", ErrIllegalArguments, txID)
		}

		return uint64(txID - 1), nil
	} else {

		var ts time.Time

		if instantVal.Type() == TimestampType {
			ts = instantVal.RawValue().(time.Time)
		} else {
			conv, err := getConverter(instantVal.Type(), TimestampType)
			if err != nil {
				return 0, err
			}

			tval, err := conv(instantVal)
			if err != nil {
				return 0, err
			}

			ts = tval.RawValue().(time.Time)
		}

		sts := ts

		if asc {
			if !inclusive {
				sts = sts.Add(1 * time.Second)
			}

			txHdr, err := tx.engine.store.FirstTxSince(sts)
			if err != nil {
				return 0, err
			}

			return txHdr.ID, nil
		}

		if !inclusive {
			sts = sts.Add(-1 * time.Second)
		}

		txHdr, err := tx.engine.store.LastTxUntil(sts)
		if err != nil {
			return 0, err
		}

		return txHdr.ID, nil
	}
}

func (stmt *tableRef) referencedTable(tx *SQLTx) (*Table, error) {
	table, err := tx.catalog.GetTableByName(stmt.table)
	if err != nil {
		return nil, err
	}
	return table, nil
}

func (stmt *tableRef) inferParameters(ctx context.Context, tx *SQLTx, params map[string]SQLValueType) error {
	return nil
}

func (stmt *tableRef) execAt(ctx context.Context, tx *SQLTx, params map[string]interface{}) (*SQLTx, error) {
	return tx, nil
}

func (stmt *tableRef) Resolve(ctx context.Context, tx *SQLTx, params map[string]interface{}, scanSpecs *ScanSpecs) (RowReader, error) {
	if tx == nil {
		return nil, ErrIllegalArguments
	}

	table, err := stmt.referencedTable(tx)
	if err == nil {
		if stmt.diff {
			return newDiffRowReader(tx, params, table, stmt.period, stmt.as, scanSpecs)
		}
		return newRawRowReader(tx, params, table, stmt.period, stmt.as, scanSpecs)
	}

	if resolver := tx.engine.tableResolveFor(stmt.table); resolver != nil {
		return resolver.Resolve(ctx, tx, stmt.Alias())
	}
	return nil, err
}

func (stmt *tableRef) Alias() string {
	if stmt.as == "" {
		return stmt.table
	}
	return stmt.as
}

type valuesDataSource struct {
	inferTypes bool
	rows       []*RowSpec
}

func NewValuesDataSource(rows []*RowSpec) *valuesDataSource {
	return &valuesDataSource{
		rows: rows,
	}
}

func (ds *valuesDataSource) readOnly() bool {
	return true
}

func (ds *valuesDataSource) requiredPrivileges() []SQLPrivilege {
	return nil
}

func (ds *valuesDataSource) execAt(ctx context.Context, tx *SQLTx, params map[string]interface{}) (*SQLTx, error) {
	return tx, nil
}

func (ds *valuesDataSource) inferParameters(ctx context.Context, tx *SQLTx, params map[string]SQLValueType) error {
	return nil
}

func (ds *valuesDataSource) Alias() string {
	return ""
}

func (ds *valuesDataSource) Resolve(ctx context.Context, tx *SQLTx, params map[string]interface{}, scanSpecs *ScanSpecs) (RowReader, error) {
	if tx == nil {
		return nil, ErrIllegalArguments
	}

	cols := make([]ColDescriptor, len(ds.rows[0].Values))
	for i := range cols {
		cols[i] = ColDescriptor{
			Type:   AnyType,
			Column: fmt.Sprintf("col%d", i),
		}
	}

	emptyColsDesc, emptyParams := map[string]ColDescriptor{}, map[string]string{}

	if ds.inferTypes {
		for i := 0; i < len(cols); i++ {
			t := AnyType
			for j := 0; j < len(ds.rows); j++ {
				e, err := ds.rows[j].Values[i].substitute(params)
				if err != nil {
					return nil, err
				}

				it, err := e.inferType(emptyColsDesc, emptyParams, "")
				if err != nil {
					return nil, err
				}

				if t == AnyType {
					t = it
				} else if t != it && it != AnyType {
					return nil, fmt.Errorf("cannot match types %s and %s", t, it)
				}
			}
			cols[i].Type = t
		}
	}

	values := make([][]ValueExp, len(ds.rows))
	for i, rowSpec := range ds.rows {
		values[i] = rowSpec.Values
	}
	return NewValuesRowReader(tx, params, cols, ds.inferTypes, "values", values)
}

type JoinSpec struct {
	joinType JoinType
	ds       DataSource
	cond     ValueExp
	indexOn  []string
	natural  bool // NATURAL JOIN — condition is built at resolve time from matching column names
	lateral  bool // LATERAL — subquery can reference columns from preceding FROM items
}

type NullsOrder int

const (
	NullsDefault NullsOrder = iota
	NullsFirst
	NullsLast
)

type OrdExp struct {
	exp        ValueExp
	descOrder  bool
	nullsOrder NullsOrder
}

func (oc *OrdExp) AsSelector() Selector {
	sel, ok := oc.exp.(Selector)
	if ok {
		return sel
	}
	return nil
}

func NewOrdCol(table string, col string, descOrder bool) *OrdExp {
	return &OrdExp{
		exp:       NewColSelector(table, col),
		descOrder: descOrder,
	}
}

type Selector interface {
	ValueExp
	resolve(implicitTable string) (aggFn, table, col string)
}

type ColSelector struct {
	table string
	col   string
}

func NewColSelector(table, col string) *ColSelector {
	return &ColSelector{
		table: table,
		col:   col,
	}
}

func (sel *ColSelector) resolve(implicitTable string) (aggFn, table, col string) {
	table = implicitTable
	if sel.table != "" {
		table = sel.table
	}
	return "", table, sel.col
}

func (sel *ColSelector) inferType(cols map[string]ColDescriptor, params map[string]SQLValueType, implicitTable string) (SQLValueType, error) {
	_, table, col := sel.resolve(implicitTable)
	encSel := EncodeSelector("", table, col)

	desc, ok := cols[encSel]
	if !ok {
		return AnyType, fmt.Errorf("%w (%s)", ErrColumnDoesNotExist, col)
	}
	return desc.Type, nil
}

func (sel *ColSelector) requiresType(t SQLValueType, cols map[string]ColDescriptor, params map[string]SQLValueType, implicitTable string) error {
	_, table, col := sel.resolve(implicitTable)
	encSel := EncodeSelector("", table, col)

	desc, ok := cols[encSel]
	if !ok {
		return fmt.Errorf("%w (%s)", ErrColumnDoesNotExist, col)
	}

	if desc.Type != t {
		return fmt.Errorf("%w: %v(%s) can not be interpreted as type %v", ErrInvalidTypes, desc.Type, encSel, t)
	}

	return nil
}

func (sel *ColSelector) substitute(params map[string]interface{}) (ValueExp, error) {
	return sel, nil
}

func (sel *ColSelector) reduce(tx *SQLTx, row *Row, implicitTable string) (TypedValue, error) {
	if row == nil {
		return nil, fmt.Errorf("%w: no row to evaluate in current context", ErrInvalidValue)
	}

	aggFn, table, col := sel.resolve(implicitTable)

	v, ok := row.ValuesBySelector[EncodeSelector(aggFn, table, col)]
	if !ok {
		return nil, fmt.Errorf("%w (%s)", ErrColumnDoesNotExist, col)
	}
	return v, nil
}

func (sel *ColSelector) selectors() []Selector {
	return []Selector{sel}
}

func (sel *ColSelector) reduceSelectors(row *Row, implicitTable string) ValueExp {
	aggFn, table, col := sel.resolve(implicitTable)

	v, ok := row.ValuesBySelector[EncodeSelector(aggFn, table, col)]
	if !ok {
		return sel
	}

	return v
}

func (sel *ColSelector) isConstant() bool {
	return false
}

func (sel *ColSelector) selectorRanges(table *Table, asTable string, params map[string]interface{}, rangesByColID map[uint32]*typedValueRange) error {
	return nil
}

func (sel *ColSelector) String() string {
	return sel.col
}

// Selector returns the encoded form "aggFn(table.col)" used as keys in
// Row.ValuesBySelector. Only meaningful when the table alias is explicit
// (e.g. in a JOIN condition like "r.customer_id = c.customer_id").
func (sel *ColSelector) Selector() string {
	return EncodeSelector("", sel.table, sel.col)
}

type AggColSelector struct {
	aggFn     AggregateFn
	table     string
	col       string
	distinct  bool
	separator string // for STRING_AGG(col, separator)
}

func NewAggColSelector(aggFn AggregateFn, table, col string) *AggColSelector {
	return &AggColSelector{
		aggFn: aggFn,
		table: table,
		col:   col,
	}
}

func EncodeSelector(aggFn, table, col string) string {
	return aggFn + "(" + table + "." + col + ")"
}

func (sel *AggColSelector) resolve(implicitTable string) (aggFn, table, col string) {
	table = implicitTable
	if sel.table != "" {
		table = sel.table
	}
	return sel.aggFn, table, sel.col
}

func (sel *AggColSelector) inferType(cols map[string]ColDescriptor, params map[string]SQLValueType, implicitTable string) (SQLValueType, error) {
	if sel.aggFn == COUNT {
		return IntegerType, nil
	}

	colSelector := &ColSelector{table: sel.table, col: sel.col}

	if sel.aggFn == SUM || sel.aggFn == AVG {
		t, err := colSelector.inferType(cols, params, implicitTable)
		if err != nil {
			return AnyType, err
		}

		if t != IntegerType && t != Float64Type {
			return AnyType, fmt.Errorf("%w: %v or %v can not be interpreted as type %v", ErrInvalidTypes, IntegerType, Float64Type, t)

		}

		return t, nil
	}

	return colSelector.inferType(cols, params, implicitTable)
}

func (sel *AggColSelector) requiresType(t SQLValueType, cols map[string]ColDescriptor, params map[string]SQLValueType, implicitTable string) error {
	if sel.aggFn == COUNT {
		if t != IntegerType {
			return fmt.Errorf("%w: %v can not be interpreted as type %v", ErrInvalidTypes, IntegerType, t)
		}
		return nil
	}

	colSelector := &ColSelector{table: sel.table, col: sel.col}

	if sel.aggFn == SUM || sel.aggFn == AVG {
		if t != IntegerType && t != Float64Type {
			return fmt.Errorf("%w: %v or %v can not be interpreted as type %v", ErrInvalidTypes, IntegerType, Float64Type, t)
		}
	}

	return colSelector.requiresType(t, cols, params, implicitTable)
}

func (sel *AggColSelector) substitute(params map[string]interface{}) (ValueExp, error) {
	return sel, nil
}

func (sel *AggColSelector) reduce(tx *SQLTx, row *Row, implicitTable string) (TypedValue, error) {
	if row == nil {
		return nil, fmt.Errorf("%w: no row to evaluate aggregation (%s) in current context", ErrInvalidValue, sel.aggFn)
	}

	v, ok := row.ValuesBySelector[EncodeSelector(sel.resolve(implicitTable))]
	if !ok {
		return nil, fmt.Errorf("%w (%s)", ErrColumnDoesNotExist, sel.col)
	}
	return v, nil
}

func (sel *AggColSelector) selectors() []Selector {
	return []Selector{sel}
}

func (sel *AggColSelector) reduceSelectors(row *Row, implicitTable string) ValueExp {
	return sel
}

func (sel *AggColSelector) isConstant() bool {
	return false
}

func (sel *AggColSelector) selectorRanges(table *Table, asTable string, params map[string]interface{}, rangesByColID map[uint32]*typedValueRange) error {
	return nil
}

func (sel *AggColSelector) String() string {
	return sel.aggFn + "(" + sel.col + ")"
}

type NumExp struct {
	op          NumOperator
	left, right ValueExp
}

func (bexp *NumExp) inferType(cols map[string]ColDescriptor, params map[string]SQLValueType, implicitTable string) (SQLValueType, error) {
	// First step - check if we can infer the type of sub-expressions
	tleft, err := bexp.left.inferType(cols, params, implicitTable)
	if err != nil {
		return AnyType, err
	}
	if tleft != AnyType && tleft != IntegerType && tleft != Float64Type && tleft != JSONType {
		return AnyType, fmt.Errorf("%w: %v or %v can not be interpreted as type %v", ErrInvalidTypes, IntegerType, Float64Type, tleft)
	}

	tright, err := bexp.right.inferType(cols, params, implicitTable)
	if err != nil {
		return AnyType, err
	}
	if tright != AnyType && tright != IntegerType && tright != Float64Type && tright != JSONType {
		return AnyType, fmt.Errorf("%w: %v or %v can not be interpreted as type %v", ErrInvalidTypes, IntegerType, Float64Type, tright)
	}

	if tleft == IntegerType && tright == IntegerType {
		// Both sides are integer types - the result is also integer
		return IntegerType, nil
	}

	if tleft != AnyType && tright != AnyType {
		// Both sides have concrete types but at least one of them is float
		return Float64Type, nil
	}

	// Both sides are ambiguous
	return AnyType, nil
}

func copyParams(params map[string]SQLValueType) map[string]SQLValueType {
	ret := make(map[string]SQLValueType, len(params))
	for k, v := range params {
		ret[k] = v
	}
	return ret
}

func restoreParams(params, restore map[string]SQLValueType) {
	for k := range params {
		delete(params, k)
	}
	for k, v := range restore {
		params[k] = v
	}
}

func (bexp *NumExp) requiresType(t SQLValueType, cols map[string]ColDescriptor, params map[string]SQLValueType, implicitTable string) error {
	if t != IntegerType && t != Float64Type {
		return fmt.Errorf("%w: %v can not be interpreted as type %v", ErrInvalidTypes, IntegerType, t)
	}

	floatArgs := 2
	paramsOrig := copyParams(params)
	err := bexp.left.requiresType(t, cols, params, implicitTable)
	if err != nil && t == Float64Type {
		restoreParams(params, paramsOrig)
		floatArgs--
		err = bexp.left.requiresType(IntegerType, cols, params, implicitTable)
	}
	if err != nil {
		return err
	}

	paramsOrig = copyParams(params)
	err = bexp.right.requiresType(t, cols, params, implicitTable)
	if err != nil && t == Float64Type {
		restoreParams(params, paramsOrig)
		floatArgs--
		err = bexp.right.requiresType(IntegerType, cols, params, implicitTable)
	}
	if err != nil {
		return err
	}

	if t == Float64Type && floatArgs == 0 {
		// Currently this case requires explicit float cast
		return fmt.Errorf("%w: %v can not be interpreted as type %v", ErrInvalidTypes, IntegerType, t)
	}

	return nil
}

func (bexp *NumExp) substitute(params map[string]interface{}) (ValueExp, error) {
	rlexp, err := bexp.left.substitute(params)
	if err != nil {
		return nil, err
	}

	rrexp, err := bexp.right.substitute(params)
	if err != nil {
		return nil, err
	}

	bexp.left = rlexp
	bexp.right = rrexp

	return bexp, nil
}

func (bexp *NumExp) reduce(tx *SQLTx, row *Row, implicitTable string) (TypedValue, error) {
	vl, err := bexp.left.reduce(tx, row, implicitTable)
	if err != nil {
		return nil, err
	}

	vr, err := bexp.right.reduce(tx, row, implicitTable)
	if err != nil {
		return nil, err
	}

	vl = unwrapJSON(vl)
	vr = unwrapJSON(vr)

	return applyNumOperator(bexp.op, vl, vr)
}

func unwrapJSON(v TypedValue) TypedValue {
	if jsonVal, ok := v.(*JSON); ok {
		if sv, isSimple := jsonVal.castToTypedValue(); isSimple {
			return sv
		}
	}
	return v
}

func (bexp *NumExp) selectors() []Selector {
	return append(bexp.left.selectors(), bexp.right.selectors()...)
}

func (bexp *NumExp) reduceSelectors(row *Row, implicitTable string) ValueExp {
	return &NumExp{
		op:    bexp.op,
		left:  bexp.left.reduceSelectors(row, implicitTable),
		right: bexp.right.reduceSelectors(row, implicitTable),
	}
}

func (bexp *NumExp) isConstant() bool {
	return bexp.left.isConstant() && bexp.right.isConstant()
}

func (bexp *NumExp) selectorRanges(table *Table, asTable string, params map[string]interface{}, rangesByColID map[uint32]*typedValueRange) error {
	return nil
}

func (bexp *NumExp) String() string {
	return fmt.Sprintf("(%s %s %s)", bexp.left.String(), NumOperatorString(bexp.op), bexp.right.String())
}

type NotBoolExp struct {
	exp ValueExp
}

func (bexp *NotBoolExp) inferType(cols map[string]ColDescriptor, params map[string]SQLValueType, implicitTable string) (SQLValueType, error) {
	err := bexp.exp.requiresType(BooleanType, cols, params, implicitTable)
	if err != nil {
		return AnyType, err
	}

	return BooleanType, nil
}

func (bexp *NotBoolExp) requiresType(t SQLValueType, cols map[string]ColDescriptor, params map[string]SQLValueType, implicitTable string) error {
	if t != BooleanType {
		return fmt.Errorf("%w: %v can not be interpreted as type %v", ErrInvalidTypes, BooleanType, t)
	}

	return bexp.exp.requiresType(BooleanType, cols, params, implicitTable)
}

func (bexp *NotBoolExp) substitute(params map[string]interface{}) (ValueExp, error) {
	rexp, err := bexp.exp.substitute(params)
	if err != nil {
		return nil, err
	}

	bexp.exp = rexp

	return bexp, nil
}

func (bexp *NotBoolExp) reduce(tx *SQLTx, row *Row, implicitTable string) (TypedValue, error) {
	v, err := bexp.exp.reduce(tx, row, implicitTable)
	if err != nil {
		return nil, err
	}

	r, isBool := v.RawValue().(bool)
	if !isBool {
		return nil, ErrInvalidCondition
	}

	return &Bool{val: !r}, nil
}

func (bexp *NotBoolExp) selectors() []Selector {
	return bexp.exp.selectors()
}

func (bexp *NotBoolExp) reduceSelectors(row *Row, implicitTable string) ValueExp {
	return &NotBoolExp{
		exp: bexp.exp.reduceSelectors(row, implicitTable),
	}
}

func (bexp *NotBoolExp) isConstant() bool {
	return bexp.exp.isConstant()
}

func (bexp *NotBoolExp) selectorRanges(table *Table, asTable string, params map[string]interface{}, rangesByColID map[uint32]*typedValueRange) error {
	return nil
}

func (bexp *NotBoolExp) String() string {
	return fmt.Sprintf("(NOT %s)", bexp.exp.String())
}

type LikeBoolExp struct {
	val             ValueExp
	notLike         bool
	caseInsensitive bool // ILIKE
	pattern         ValueExp
}

func NewLikeBoolExp(val ValueExp, notLike bool, pattern ValueExp) *LikeBoolExp {
	return &LikeBoolExp{
		val:     val,
		notLike: notLike,
		pattern: pattern,
	}
}

func (bexp *LikeBoolExp) inferType(cols map[string]ColDescriptor, params map[string]SQLValueType, implicitTable string) (SQLValueType, error) {
	if bexp.val == nil || bexp.pattern == nil {
		return AnyType, fmt.Errorf("error in 'LIKE' clause: %w", ErrInvalidCondition)
	}

	err := bexp.pattern.requiresType(VarcharType, cols, params, implicitTable)
	if err != nil {
		return AnyType, fmt.Errorf("error in 'LIKE' clause: %w", err)
	}

	return BooleanType, nil
}

func (bexp *LikeBoolExp) requiresType(t SQLValueType, cols map[string]ColDescriptor, params map[string]SQLValueType, implicitTable string) error {
	if bexp.val == nil || bexp.pattern == nil {
		return fmt.Errorf("error in 'LIKE' clause: %w", ErrInvalidCondition)
	}

	if t != BooleanType {
		return fmt.Errorf("error using the value of the LIKE operator as %s: %w", t, ErrInvalidTypes)
	}

	err := bexp.pattern.requiresType(VarcharType, cols, params, implicitTable)
	if err != nil {
		return fmt.Errorf("error in 'LIKE' clause: %w", err)
	}

	return nil
}

func (bexp *LikeBoolExp) substitute(params map[string]interface{}) (ValueExp, error) {
	if bexp.val == nil || bexp.pattern == nil {
		return nil, fmt.Errorf("error in 'LIKE' clause: %w", ErrInvalidCondition)
	}

	val, err := bexp.val.substitute(params)
	if err != nil {
		return nil, fmt.Errorf("error in 'LIKE' clause: %w", err)
	}

	pattern, err := bexp.pattern.substitute(params)
	if err != nil {
		return nil, fmt.Errorf("error in 'LIKE' clause: %w", err)
	}

	return &LikeBoolExp{
		val:             val,
		notLike:         bexp.notLike,
		caseInsensitive: bexp.caseInsensitive,
		pattern:         pattern,
	}, nil
}

func (bexp *LikeBoolExp) reduce(tx *SQLTx, row *Row, implicitTable string) (TypedValue, error) {
	if bexp.val == nil || bexp.pattern == nil {
		return nil, fmt.Errorf("error in 'LIKE' clause: %w", ErrInvalidCondition)
	}

	rval, err := bexp.val.reduce(tx, row, implicitTable)
	if err != nil {
		return nil, fmt.Errorf("error in 'LIKE' clause: %w", err)
	}

	if rval.IsNull() {
		return &Bool{val: bexp.notLike}, nil
	}

	rvalStr, ok := rval.RawValue().(string)
	if !ok {
		return nil, fmt.Errorf("error in 'LIKE' clause: %w (expecting %s)", ErrInvalidTypes, VarcharType)
	}

	rpattern, err := bexp.pattern.reduce(tx, row, implicitTable)
	if err != nil {
		return nil, fmt.Errorf("error in 'LIKE' clause: %w", err)
	}

	if rpattern.Type() != VarcharType {
		return nil, fmt.Errorf("error evaluating 'LIKE' clause: %w", ErrInvalidTypes)
	}

	patternStr := rpattern.RawValue().(string)

	// Convert SQL LIKE wildcards to regex:
	// % -> .* (match any sequence)
	// _ -> .  (match single char)
	// Escape all other regex metacharacters
	regexPattern := sqlLikeToRegex(patternStr)

	if bexp.caseInsensitive {
		regexPattern = "(?i)" + regexPattern
	}
	matched, err := regexp.MatchString(regexPattern, rvalStr)
	if err != nil {
		return nil, fmt.Errorf("error in 'LIKE' clause: %w", err)
	}

	return &Bool{val: matched != bexp.notLike}, nil
}

// sqlLikeToRegex converts a SQL LIKE pattern to a Go regex pattern.
// SQL LIKE uses % for any sequence and _ for single char.
// All regex metacharacters in the literal parts are escaped.
// Backslash escapes in the pattern (\% and \_) are honored.
func sqlLikeToRegex(pattern string) string {
	var b strings.Builder
	b.WriteString("^")

	i := 0
	for i < len(pattern) {
		ch := pattern[i]
		switch {
		case ch == '\\' && i+1 < len(pattern):
			// Escaped character — treat next char as literal
			next := pattern[i+1]
			b.WriteString(regexp.QuoteMeta(string(next)))
			i += 2
		case ch == '%':
			b.WriteString(".*")
			i++
		case ch == '_':
			b.WriteString(".")
			i++
		default:
			b.WriteString(regexp.QuoteMeta(string(ch)))
			i++
		}
	}

	b.WriteString("$")
	return b.String()
}

func (bexp *LikeBoolExp) selectors() []Selector {
	return bexp.val.selectors()
}

func (bexp *LikeBoolExp) reduceSelectors(row *Row, implicitTable string) ValueExp {
	return bexp
}

func (bexp *LikeBoolExp) isConstant() bool {
	return false
}

func (bexp *LikeBoolExp) selectorRanges(table *Table, asTable string, params map[string]interface{}, rangesByColID map[uint32]*typedValueRange) error {
	return nil
}

func (bexp *LikeBoolExp) String() string {
	op := "LIKE"
	if bexp.caseInsensitive {
		op = "ILIKE"
	}
	if bexp.notLike {
		op = "NOT " + op
	}
	return fmt.Sprintf("(%s %s %s)", bexp.val.String(), op, bexp.pattern.String())
}

type CmpBoolExp struct {
	op          CmpOperator
	left, right ValueExp
}

func NewCmpBoolExp(op CmpOperator, left, right ValueExp) *CmpBoolExp {
	return &CmpBoolExp{
		op:    op,
		left:  left,
		right: right,
	}
}

func (bexp *CmpBoolExp) Left() ValueExp {
	return bexp.left
}

func (bexp *CmpBoolExp) Right() ValueExp {
	return bexp.right
}

func (bexp *CmpBoolExp) OP() CmpOperator {
	return bexp.op
}

func (bexp *CmpBoolExp) inferType(cols map[string]ColDescriptor, params map[string]SQLValueType, implicitTable string) (SQLValueType, error) {
	tleft, err := bexp.left.inferType(cols, params, implicitTable)
	if err != nil {
		return AnyType, err
	}

	tright, err := bexp.right.inferType(cols, params, implicitTable)
	if err != nil {
		return AnyType, err
	}

	// unification step

	if tleft == tright {
		return BooleanType, nil
	}

	_, ok := coerceTypes(tleft, tright)
	if !ok {
		return AnyType, fmt.Errorf("%w: %v can not be interpreted as type %v", ErrInvalidTypes, tleft, tright)
	}

	if tleft == AnyType {
		err = bexp.left.requiresType(tright, cols, params, implicitTable)
		if err != nil {
			return AnyType, err
		}
	}

	if tright == AnyType {
		err = bexp.right.requiresType(tleft, cols, params, implicitTable)
		if err != nil {
			return AnyType, err
		}
	}
	return BooleanType, nil
}

func coerceTypes(t1, t2 SQLValueType) (SQLValueType, bool) {
	switch {
	case t1 == t2:
		return t1, true
	case t1 == AnyType:
		return t2, true
	case t2 == AnyType:
		return t1, true
	case (t1 == IntegerType && t2 == Float64Type) ||
		(t1 == Float64Type && t2 == IntegerType):
		return Float64Type, true
	}
	return "", false
}

func (bexp *CmpBoolExp) requiresType(t SQLValueType, cols map[string]ColDescriptor, params map[string]SQLValueType, implicitTable string) error {
	if t != BooleanType {
		return fmt.Errorf("%w: %v can not be interpreted as type %v", ErrInvalidTypes, BooleanType, t)
	}

	_, err := bexp.inferType(cols, params, implicitTable)
	return err
}

func (bexp *CmpBoolExp) substitute(params map[string]interface{}) (ValueExp, error) {
	rlexp, err := bexp.left.substitute(params)
	if err != nil {
		return nil, err
	}

	rrexp, err := bexp.right.substitute(params)
	if err != nil {
		return nil, err
	}

	bexp.left = rlexp
	bexp.right = rrexp

	return bexp, nil
}

func (bexp *CmpBoolExp) reduce(tx *SQLTx, row *Row, implicitTable string) (TypedValue, error) {
	vl, err := bexp.left.reduce(tx, row, implicitTable)
	if err != nil {
		return nil, err
	}

	vr, err := bexp.right.reduce(tx, row, implicitTable)
	if err != nil {
		return nil, err
	}

	r, err := vl.Compare(vr)
	if err != nil {
		return nil, err
	}

	return &Bool{val: cmpSatisfiesOp(r, bexp.op)}, nil
}

func (bexp *CmpBoolExp) selectors() []Selector {
	return append(bexp.left.selectors(), bexp.right.selectors()...)
}

func (bexp *CmpBoolExp) reduceSelectors(row *Row, implicitTable string) ValueExp {
	return &CmpBoolExp{
		op:    bexp.op,
		left:  bexp.left.reduceSelectors(row, implicitTable),
		right: bexp.right.reduceSelectors(row, implicitTable),
	}
}

func (bexp *CmpBoolExp) isConstant() bool {
	return bexp.left.isConstant() && bexp.right.isConstant()
}

func (bexp *CmpBoolExp) selectorRanges(table *Table, asTable string, params map[string]interface{}, rangesByColID map[uint32]*typedValueRange) error {
	matchingFunc := func(_, right ValueExp) (*ColSelector, ValueExp, bool) {
		s, isSel := bexp.left.(*ColSelector)
		if isSel && s.col != revCol && bexp.right.isConstant() {
			return s, right, true
		}
		return nil, nil, false
	}

	sel, c, ok := matchingFunc(bexp.left, bexp.right)
	if !ok {
		sel, c, ok = matchingFunc(bexp.right, bexp.left)
	}

	if !ok {
		return nil
	}

	aggFn, t, col := sel.resolve(table.name)
	if aggFn != "" || t != asTable {
		return nil
	}

	column, err := table.GetColumnByName(col)
	if err != nil {
		return err
	}

	val, err := c.substitute(params)
	if errors.Is(err, ErrMissingParameter) {
		// TODO: not supported when parameters are not provided during query resolution
		return nil
	}
	if err != nil {
		return err
	}

	rval, err := val.reduce(nil, nil, table.name)
	if err != nil {
		return err
	}

	return updateRangeFor(column.id, rval, bexp.op, rangesByColID)
}

func (bexp *CmpBoolExp) String() string {
	opStr := CmpOperatorToString(bexp.op)
	return fmt.Sprintf("(%s %s %s)", bexp.left.String(), opStr, bexp.right.String())
}

type TimestampFieldType string

const (
	TimestampFieldTypeYear   TimestampFieldType = "YEAR"
	TimestampFieldTypeMonth  TimestampFieldType = "MONTH"
	TimestampFieldTypeDay    TimestampFieldType = "DAY"
	TimestampFieldTypeHour   TimestampFieldType = "HOUR"
	TimestampFieldTypeMinute TimestampFieldType = "MINUTE"
	TimestampFieldTypeSecond TimestampFieldType = "SECOND"
)

type ExtractFromTimestampExp struct {
	Field TimestampFieldType
	Exp   ValueExp
}

func (te *ExtractFromTimestampExp) inferType(cols map[string]ColDescriptor, params map[string]SQLValueType, implicitTable string) (SQLValueType, error) {
	inferredType, err := te.Exp.inferType(cols, params, implicitTable)
	if err != nil {
		return "", err
	}

	if inferredType != TimestampType &&
		inferredType != VarcharType &&
		inferredType != AnyType {
		return "", fmt.Errorf("timestamp expression must be of type %v or %v, but was: %v", TimestampType, VarcharType, inferredType)
	}
	return IntegerType, nil
}

func (te *ExtractFromTimestampExp) requiresType(t SQLValueType, cols map[string]ColDescriptor, params map[string]SQLValueType, implicitTable string) error {
	if t != IntegerType && t != Float64Type {
		return fmt.Errorf("%w: %v can not be interpreted as type %v", ErrInvalidTypes, BooleanType, t)
	}
	return te.Exp.requiresType(TimestampType, cols, params, implicitTable)
}

func (te *ExtractFromTimestampExp) substitute(params map[string]interface{}) (ValueExp, error) {
	exp, err := te.Exp.substitute(params)
	if err != nil {
		return nil, err
	}
	return &ExtractFromTimestampExp{
		Field: te.Field,
		Exp:   exp,
	}, nil
}

func (te *ExtractFromTimestampExp) selectors() []Selector {
	return te.Exp.selectors()
}

func (te *ExtractFromTimestampExp) reduce(tx *SQLTx, row *Row, implicitTable string) (TypedValue, error) {
	v, err := te.Exp.reduce(tx, row, implicitTable)
	if err != nil {
		return nil, err
	}

	if v.IsNull() {
		return NewNull(IntegerType), nil
	}

	if t := v.Type(); t != TimestampType && t != VarcharType {
		return nil, fmt.Errorf("%w: expected type %v but found type %v", ErrInvalidTypes, TimestampType, t)
	}

	if v.Type() == VarcharType {
		converterFunc, err := getConverter(VarcharType, TimestampType)
		if err != nil {
			return nil, err
		}
		casted, err := converterFunc(v)
		if err != nil {
			return nil, err
		}
		v = casted
	}

	t, _ := v.RawValue().(time.Time)

	year, month, day := t.Date()

	switch te.Field {
	case TimestampFieldTypeYear:
		return NewInteger(int64(year)), nil
	case TimestampFieldTypeMonth:
		return NewInteger(int64(month)), nil
	case TimestampFieldTypeDay:
		return NewInteger(int64(day)), nil
	case TimestampFieldTypeHour:
		return NewInteger(int64(t.Hour())), nil
	case TimestampFieldTypeMinute:
		return NewInteger(int64(t.Minute())), nil
	case TimestampFieldTypeSecond:
		return NewInteger(int64(t.Second())), nil
	}
	return nil, fmt.Errorf("unknown timestamp field type: %s", te.Field)
}

func (te *ExtractFromTimestampExp) reduceSelectors(row *Row, implicitTable string) ValueExp {
	return &ExtractFromTimestampExp{
		Field: te.Field,
		Exp:   te.Exp.reduceSelectors(row, implicitTable),
	}
}

func (te *ExtractFromTimestampExp) isConstant() bool {
	return false
}

func (te *ExtractFromTimestampExp) selectorRanges(table *Table, asTable string, params map[string]interface{}, rangesByColID map[uint32]*typedValueRange) error {
	return nil
}

func (te *ExtractFromTimestampExp) String() string {
	return fmt.Sprintf("EXTRACT(%s FROM %s)", te.Field, te.Exp)
}

func updateRangeFor(colID uint32, val TypedValue, cmp CmpOperator, rangesByColID map[uint32]*typedValueRange) error {
	currRange, ranged := rangesByColID[colID]
	var newRange *typedValueRange

	switch cmp {
	case EQ:
		{
			newRange = &typedValueRange{
				lRange: &typedValueSemiRange{
					val:       val,
					inclusive: true,
				},
				hRange: &typedValueSemiRange{
					val:       val,
					inclusive: true,
				},
			}
		}
	case LT:
		{
			newRange = &typedValueRange{
				hRange: &typedValueSemiRange{
					val: val,
				},
			}
		}
	case LE:
		{
			newRange = &typedValueRange{
				hRange: &typedValueSemiRange{
					val:       val,
					inclusive: true,
				},
			}
		}
	case GT:
		{
			newRange = &typedValueRange{
				lRange: &typedValueSemiRange{
					val: val,
				},
			}
		}
	case GE:
		{
			newRange = &typedValueRange{
				lRange: &typedValueSemiRange{
					val:       val,
					inclusive: true,
				},
			}
		}
	case NE:
		{
			return nil
		}
	}

	if !ranged {
		rangesByColID[colID] = newRange
		return nil
	}

	return currRange.refineWith(newRange)
}

func cmpSatisfiesOp(cmp int, op CmpOperator) bool {
	switch {
	case cmp == 0:
		{
			return op == EQ || op == LE || op == GE
		}
	case cmp < 0:
		{
			return op == NE || op == LT || op == LE
		}
	case cmp > 0:
		{
			return op == NE || op == GT || op == GE
		}
	}
	return false
}

type BinBoolExp struct {
	op          LogicOperator
	left, right ValueExp
}

func NewBinBoolExp(op LogicOperator, lrexp, rrexp ValueExp) *BinBoolExp {
	bexp := &BinBoolExp{
		op: op,
	}

	bexp.left = lrexp
	bexp.right = rrexp

	return bexp
}

func (bexp *BinBoolExp) inferType(cols map[string]ColDescriptor, params map[string]SQLValueType, implicitTable string) (SQLValueType, error) {
	err := bexp.left.requiresType(BooleanType, cols, params, implicitTable)
	if err != nil {
		return AnyType, err
	}

	err = bexp.right.requiresType(BooleanType, cols, params, implicitTable)
	if err != nil {
		return AnyType, err
	}

	return BooleanType, nil
}

func (bexp *BinBoolExp) requiresType(t SQLValueType, cols map[string]ColDescriptor, params map[string]SQLValueType, implicitTable string) error {
	if t != BooleanType {
		return fmt.Errorf("%w: %v can not be interpreted as type %v", ErrInvalidTypes, BooleanType, t)
	}

	err := bexp.left.requiresType(BooleanType, cols, params, implicitTable)
	if err != nil {
		return err
	}

	err = bexp.right.requiresType(BooleanType, cols, params, implicitTable)
	if err != nil {
		return err
	}

	return nil
}

func (bexp *BinBoolExp) substitute(params map[string]interface{}) (ValueExp, error) {
	rlexp, err := bexp.left.substitute(params)
	if err != nil {
		return nil, err
	}

	rrexp, err := bexp.right.substitute(params)
	if err != nil {
		return nil, err
	}

	bexp.left = rlexp
	bexp.right = rrexp

	return bexp, nil
}

func (bexp *BinBoolExp) reduce(tx *SQLTx, row *Row, implicitTable string) (TypedValue, error) {
	vl, err := bexp.left.reduce(tx, row, implicitTable)
	if err != nil {
		return nil, err
	}

	bl, isBool := vl.(*Bool)
	if !isBool {
		return nil, fmt.Errorf("%w (expecting boolean value)", ErrInvalidValue)
	}

	// short-circuit evaluation
	if (bl.val && bexp.op == Or) || (!bl.val && bexp.op == And) {
		return &Bool{val: bl.val}, nil
	}

	vr, err := bexp.right.reduce(tx, row, implicitTable)
	if err != nil {
		return nil, err
	}

	br, isBool := vr.(*Bool)
	if !isBool {
		return nil, fmt.Errorf("%w (expecting boolean value)", ErrInvalidValue)
	}

	switch bexp.op {
	case And:
		{
			return &Bool{val: bl.val && br.val}, nil
		}
	case Or:
		{
			return &Bool{val: bl.val || br.val}, nil
		}
	}

	return nil, ErrUnexpected
}

func (bexp *BinBoolExp) selectors() []Selector {
	return append(bexp.left.selectors(), bexp.right.selectors()...)
}

func (bexp *BinBoolExp) reduceSelectors(row *Row, implicitTable string) ValueExp {
	return &BinBoolExp{
		op:    bexp.op,
		left:  bexp.left.reduceSelectors(row, implicitTable),
		right: bexp.right.reduceSelectors(row, implicitTable),
	}
}

func (bexp *BinBoolExp) isConstant() bool {
	return bexp.left.isConstant() && bexp.right.isConstant()
}

func (bexp *BinBoolExp) selectorRanges(table *Table, asTable string, params map[string]interface{}, rangesByColID map[uint32]*typedValueRange) error {
	if bexp.op == And {
		err := bexp.left.selectorRanges(table, asTable, params, rangesByColID)
		if err != nil {
			return err
		}

		return bexp.right.selectorRanges(table, asTable, params, rangesByColID)
	}

	lRanges := make(map[uint32]*typedValueRange)
	rRanges := make(map[uint32]*typedValueRange)

	err := bexp.left.selectorRanges(table, asTable, params, lRanges)
	if err != nil {
		return err
	}

	err = bexp.right.selectorRanges(table, asTable, params, rRanges)
	if err != nil {
		return err
	}

	for colID, lr := range lRanges {
		rr, ok := rRanges[colID]
		if !ok {
			continue
		}

		err = lr.extendWith(rr)
		if err != nil {
			return err
		}

		rangesByColID[colID] = lr
	}

	return nil
}

func (bexp *BinBoolExp) String() string {
	return fmt.Sprintf("(%s %s %s)", bexp.left.String(), LogicOperatorToString(bexp.op), bexp.right.String())
}

type ExistsBoolExp struct {
	q DataSource
}

func (bexp *ExistsBoolExp) inferType(cols map[string]ColDescriptor, params map[string]SQLValueType, implicitTable string) (SQLValueType, error) {
	return BooleanType, nil
}

func (bexp *ExistsBoolExp) requiresType(t SQLValueType, cols map[string]ColDescriptor, params map[string]SQLValueType, implicitTable string) error {
	if t != BooleanType {
		return fmt.Errorf("%w: %v can not be interpreted as type %v", ErrInvalidTypes, BooleanType, t)
	}
	return nil
}

func (bexp *ExistsBoolExp) substitute(params map[string]interface{}) (ValueExp, error) {
	return bexp, nil
}

func (bexp *ExistsBoolExp) reduce(tx *SQLTx, row *Row, implicitTable string) (TypedValue, error) {
	if tx == nil {
		return nil, fmt.Errorf("'EXISTS' clause: %w", ErrNoSupported)
	}

	// For correlated subqueries, reduce the inner query's selectors using the outer row
	ds := bexp.q
	if row != nil {
		if sel, ok := ds.(*SelectStmt); ok && sel.where != nil {
			ds = &SelectStmt{
				ds:      sel.ds,
				targets: sel.targets,
				where:   sel.where.reduceSelectors(row, implicitTable),
				joins:   sel.joins,
				indexOn: sel.indexOn,
			}
		}
	}

	reader, err := ds.Resolve(tx.tx.Context(), tx, nil, nil)
	if err != nil {
		return nil, fmt.Errorf("error in 'EXISTS' clause: %w", err)
	}
	defer reader.Close()

	_, err = reader.Read(tx.tx.Context())
	if err != nil {
		if err == ErrNoMoreRows {
			return &Bool{val: false}, nil
		}
		return nil, fmt.Errorf("error in 'EXISTS' clause: %w", err)
	}

	return &Bool{val: true}, nil
}

func (bexp *ExistsBoolExp) selectors() []Selector {
	return nil
}

func (bexp *ExistsBoolExp) reduceSelectors(row *Row, implicitTable string) ValueExp {
	return bexp
}

func (bexp *ExistsBoolExp) isConstant() bool {
	return false
}

func (bexp *ExistsBoolExp) selectorRanges(table *Table, asTable string, params map[string]interface{}, rangesByColID map[uint32]*typedValueRange) error {
	return nil
}

func (bexp *ExistsBoolExp) String() string {
	return ""
}

type InSubQueryExp struct {
	val   ValueExp
	notIn bool
	q     *SelectStmt
}

func (bexp *InSubQueryExp) inferType(cols map[string]ColDescriptor, params map[string]SQLValueType, implicitTable string) (SQLValueType, error) {
	return BooleanType, nil
}

func (bexp *InSubQueryExp) requiresType(t SQLValueType, cols map[string]ColDescriptor, params map[string]SQLValueType, implicitTable string) error {
	if t != BooleanType {
		return fmt.Errorf("%w: %v can not be interpreted as type %v", ErrInvalidTypes, BooleanType, t)
	}
	return nil
}

func (bexp *InSubQueryExp) substitute(params map[string]interface{}) (ValueExp, error) {
	return bexp, nil
}

func (bexp *InSubQueryExp) reduce(tx *SQLTx, row *Row, implicitTable string) (TypedValue, error) {
	if tx == nil {
		return nil, fmt.Errorf("'IN' subquery clause: %w", ErrNoSupported)
	}

	rval, err := bexp.val.reduce(tx, row, implicitTable)
	if err != nil {
		return nil, fmt.Errorf("error evaluating 'IN' clause: %w", err)
	}

	// For correlated subqueries, reduce the inner query's selectors using the outer row
	q := bexp.q
	if row != nil && q.where != nil {
		q = &SelectStmt{
			ds:      q.ds,
			targets: q.targets,
			where:   q.where.reduceSelectors(row, implicitTable),
			joins:   q.joins,
			indexOn: q.indexOn,
		}
	}

	reader, err := q.Resolve(tx.tx.Context(), tx, nil, nil)
	if err != nil {
		return nil, fmt.Errorf("error in 'IN' subquery: %w", err)
	}
	defer reader.Close()

	cols, err := reader.Columns(tx.tx.Context())
	if err != nil {
		return nil, fmt.Errorf("error in 'IN' subquery: %w", err)
	}

	if len(cols) == 0 {
		return nil, fmt.Errorf("error in 'IN' subquery: subquery must return exactly one column")
	}

	var found bool

	for {
		subRow, err := reader.Read(tx.tx.Context())
		if err != nil {
			if err == ErrNoMoreRows {
				break
			}
			return nil, fmt.Errorf("error in 'IN' subquery: %w", err)
		}

		if len(subRow.ValuesByPosition) == 0 {
			continue
		}

		subVal := subRow.ValuesByPosition[0]
		r, err := rval.Compare(subVal)
		if err != nil {
			continue
		}

		if r == 0 {
			found = true
			break
		}
	}

	return &Bool{val: found != bexp.notIn}, nil
}

func (bexp *InSubQueryExp) selectors() []Selector {
	return bexp.val.selectors()
}

func (bexp *InSubQueryExp) reduceSelectors(row *Row, implicitTable string) ValueExp {
	return bexp
}

func (bexp *InSubQueryExp) isConstant() bool {
	return false
}

func (bexp *InSubQueryExp) selectorRanges(table *Table, asTable string, params map[string]interface{}, rangesByColID map[uint32]*typedValueRange) error {
	return nil
}

func (bexp *InSubQueryExp) String() string {
	return ""
}

// TODO: once InSubQueryExp is supported, this struct may become obsolete by creating a ListDataSource struct
type InListExp struct {
	val    ValueExp
	notIn  bool
	values []ValueExp
}

func (bexp *InListExp) inferType(cols map[string]ColDescriptor, params map[string]SQLValueType, implicitTable string) (SQLValueType, error) {
	t, err := bexp.val.inferType(cols, params, implicitTable)
	if err != nil {
		return AnyType, fmt.Errorf("error inferring type in 'IN' clause: %w", err)
	}

	for _, v := range bexp.values {
		err = v.requiresType(t, cols, params, implicitTable)
		if err != nil {
			return AnyType, fmt.Errorf("error inferring type in 'IN' clause: %w", err)
		}
	}

	return BooleanType, nil
}

func (bexp *InListExp) requiresType(t SQLValueType, cols map[string]ColDescriptor, params map[string]SQLValueType, implicitTable string) error {
	_, err := bexp.inferType(cols, params, implicitTable)
	if err != nil {
		return err
	}

	if t != BooleanType {
		return fmt.Errorf("error inferring type in 'IN' clause: %w", ErrInvalidTypes)
	}

	return nil
}

func (bexp *InListExp) substitute(params map[string]interface{}) (ValueExp, error) {
	val, err := bexp.val.substitute(params)
	if err != nil {
		return nil, fmt.Errorf("error evaluating 'IN' clause: %w", err)
	}

	values := make([]ValueExp, len(bexp.values))

	for i, val := range bexp.values {
		values[i], err = val.substitute(params)
		if err != nil {
			return nil, fmt.Errorf("error evaluating 'IN' clause: %w", err)
		}
	}

	return &InListExp{
		val:    val,
		notIn:  bexp.notIn,
		values: values,
	}, nil
}

func (bexp *InListExp) reduce(tx *SQLTx, row *Row, implicitTable string) (TypedValue, error) {
	rval, err := bexp.val.reduce(tx, row, implicitTable)
	if err != nil {
		return nil, fmt.Errorf("error evaluating 'IN' clause: %w", err)
	}

	var found bool

	for _, v := range bexp.values {
		rv, err := v.reduce(tx, row, implicitTable)
		if err != nil {
			return nil, fmt.Errorf("error evaluating 'IN' clause: %w", err)
		}

		r, err := rval.Compare(rv)
		if err != nil {
			return nil, fmt.Errorf("error evaluating 'IN' clause: %w", err)
		}

		if r == 0 {
			// TODO: short-circuit evaluation may be preferred when upfront static type inference is in place
			found = found || true
		}
	}

	return &Bool{val: found != bexp.notIn}, nil
}

func (bexp *InListExp) selectors() []Selector {
	selectors := make([]Selector, 0, len(bexp.values))
	for _, v := range bexp.values {
		selectors = append(selectors, v.selectors()...)
	}
	return append(bexp.val.selectors(), selectors...)
}

func (bexp *InListExp) reduceSelectors(row *Row, implicitTable string) ValueExp {
	values := make([]ValueExp, len(bexp.values))

	for i, val := range bexp.values {
		values[i] = val.reduceSelectors(row, implicitTable)
	}

	return &InListExp{
		val:    bexp.val.reduceSelectors(row, implicitTable),
		values: values,
	}
}

func (bexp *InListExp) isConstant() bool {
	return false
}

func (bexp *InListExp) selectorRanges(table *Table, asTable string, params map[string]interface{}, rangesByColID map[uint32]*typedValueRange) error {
	// TODO: may be determiined by smallest and bigggest value in the list
	return nil
}

func (bexp *InListExp) String() string {
	values := make([]string, len(bexp.values))
	for i, exp := range bexp.values {
		values[i] = exp.String()
	}
	return fmt.Sprintf("%s IN (%s)", bexp.val.String(), strings.Join(values, ","))
}

type FnDataSourceStmt struct {
	fnCall *FnCall
	as     string
}

func (stmt *FnDataSourceStmt) readOnly() bool {
	return true
}

func (stmt *FnDataSourceStmt) requiredPrivileges() []SQLPrivilege {
	return nil
}

func (stmt *FnDataSourceStmt) execAt(ctx context.Context, tx *SQLTx, params map[string]interface{}) (*SQLTx, error) {
	return tx, nil
}

func (stmt *FnDataSourceStmt) inferParameters(ctx context.Context, tx *SQLTx, params map[string]SQLValueType) error {
	return nil
}

func (stmt *FnDataSourceStmt) Alias() string {
	if stmt.as != "" {
		return stmt.as
	}

	switch strings.ToUpper(stmt.fnCall.fn) {
	case DatabasesFnCall:
		{
			return "databases"
		}
	case TablesFnCall:
		{
			return "tables"
		}
	case TableFnCall:
		{
			return "table"
		}
	case UsersFnCall:
		{
			return "users"
		}
	case ColumnsFnCall:
		{
			return "columns"
		}
	case IndexesFnCall:
		{
			return "indexes"
		}
	case GrantsFnCall:
		return "grants"
	}

	// not reachable
	return ""
}

func (stmt *FnDataSourceStmt) Resolve(ctx context.Context, tx *SQLTx, params map[string]interface{}, scanSpecs *ScanSpecs) (rowReader RowReader, err error) {
	if stmt.fnCall == nil {
		return nil, fmt.Errorf("%w: function is unspecified", ErrIllegalArguments)
	}

	switch strings.ToUpper(stmt.fnCall.fn) {
	case DatabasesFnCall:
		{
			return stmt.resolveListDatabases(ctx, tx, params, scanSpecs)
		}
	case TablesFnCall:
		{
			return stmt.resolveListTables(ctx, tx, params, scanSpecs)
		}
	case TableFnCall:
		{
			return stmt.resolveShowTable(ctx, tx, params, scanSpecs)
		}
	case UsersFnCall:
		{
			return stmt.resolveListUsers(ctx, tx, params, scanSpecs)
		}
	case ColumnsFnCall:
		{
			return stmt.resolveListColumns(ctx, tx, params, scanSpecs)
		}
	case IndexesFnCall:
		{
			return stmt.resolveListIndexes(ctx, tx, params, scanSpecs)
		}
	case GrantsFnCall:
		{
			return stmt.resolveListGrants(ctx, tx, params, scanSpecs)
		}
	}

	return nil, fmt.Errorf("%w (%s)", ErrFunctionDoesNotExist, stmt.fnCall.fn)
}

func (stmt *FnDataSourceStmt) resolveListDatabases(ctx context.Context, tx *SQLTx, params map[string]interface{}, _ *ScanSpecs) (rowReader RowReader, err error) {
	if len(stmt.fnCall.params) > 0 {
		return nil, fmt.Errorf("%w: function '%s' expect no parameters but %d were provided", ErrIllegalArguments, DatabasesFnCall, len(stmt.fnCall.params))
	}

	cols := make([]ColDescriptor, 1)
	cols[0] = ColDescriptor{
		Column: "name",
		Type:   VarcharType,
	}

	var dbs []string

	if tx.engine.multidbHandler == nil {
		return nil, ErrUnspecifiedMultiDBHandler
	} else {
		dbs, err = tx.engine.multidbHandler.ListDatabases(ctx)
		if err != nil {
			return nil, err
		}
	}

	values := make([][]ValueExp, len(dbs))

	for i, db := range dbs {
		values[i] = []ValueExp{&Varchar{val: db}}
	}

	return NewValuesRowReader(tx, params, cols, true, stmt.Alias(), values)
}

func (stmt *FnDataSourceStmt) resolveListTables(ctx context.Context, tx *SQLTx, params map[string]interface{}, _ *ScanSpecs) (rowReader RowReader, err error) {
	if len(stmt.fnCall.params) > 0 {
		return nil, fmt.Errorf("%w: function '%s' expect no parameters but %d were provided", ErrIllegalArguments, TablesFnCall, len(stmt.fnCall.params))
	}

	cols := make([]ColDescriptor, 1)
	cols[0] = ColDescriptor{
		Column: "name",
		Type:   VarcharType,
	}

	tables := tx.catalog.GetTables()

	values := make([][]ValueExp, len(tables))

	for i, t := range tables {
		values[i] = []ValueExp{&Varchar{val: t.name}}
	}

	return NewValuesRowReader(tx, params, cols, true, stmt.Alias(), values)
}

func (stmt *FnDataSourceStmt) resolveShowTable(ctx context.Context, tx *SQLTx, params map[string]interface{}, _ *ScanSpecs) (rowReader RowReader, err error) {
	cols := []ColDescriptor{
		{
			Column: "column_name",
			Type:   VarcharType,
		},
		{
			Column: "type_name",
			Type:   VarcharType,
		},
		{
			Column: "is_nullable",
			Type:   BooleanType,
		},
		{
			Column: "is_indexed",
			Type:   VarcharType,
		},
		{
			Column: "is_auto_increment",
			Type:   BooleanType,
		},
		{
			Column: "is_unique",
			Type:   BooleanType,
		},
	}

	tableName, _ := stmt.fnCall.params[0].reduce(tx, nil, "")
	table, err := tx.catalog.GetTableByName(tableName.RawValue().(string))
	if err != nil {
		return nil, err
	}

	values := make([][]ValueExp, len(table.cols))

	for i, c := range table.cols {
		index := "NO"

		indexed, err := table.IsIndexed(c.Name())
		if err != nil {
			return nil, err
		}
		if indexed {
			index = "YES"
		}

		if table.PrimaryIndex().IncludesCol(c.ID()) {
			index = "PRIMARY KEY"
		}

		var unique bool
		for _, index := range table.GetIndexesByColID(c.ID()) {
			if index.IsUnique() && len(index.Cols()) == 1 {
				unique = true
				break
			}
		}

		var maxLen string

		if c.MaxLen() > 0 && (c.Type() == VarcharType || c.Type() == BLOBType) {
			maxLen = fmt.Sprintf("(%d)", c.MaxLen())
		}

		values[i] = []ValueExp{
			&Varchar{val: c.colName},
			&Varchar{val: c.Type() + maxLen},
			&Bool{val: c.IsNullable()},
			&Varchar{val: index},
			&Bool{val: c.IsAutoIncremental()},
			&Bool{val: unique},
		}
	}

	return NewValuesRowReader(tx, params, cols, true, stmt.Alias(), values)
}

func (stmt *FnDataSourceStmt) resolveListUsers(ctx context.Context, tx *SQLTx, params map[string]interface{}, _ *ScanSpecs) (rowReader RowReader, err error) {
	if len(stmt.fnCall.params) > 0 {
		return nil, fmt.Errorf("%w: function '%s' expect no parameters but %d were provided", ErrIllegalArguments, UsersFnCall, len(stmt.fnCall.params))
	}

	cols := []ColDescriptor{
		{
			Column: "name",
			Type:   VarcharType,
		},
		{
			Column: "permission",
			Type:   VarcharType,
		},
	}

	users, err := tx.ListUsers(ctx)
	if err != nil {
		return nil, err
	}

	values := make([][]ValueExp, len(users))
	for i, user := range users {
		perm := user.Permission()

		values[i] = []ValueExp{
			&Varchar{val: user.Username()},
			&Varchar{val: perm},
		}
	}
	return NewValuesRowReader(tx, params, cols, true, stmt.Alias(), values)
}

func (stmt *FnDataSourceStmt) resolveListColumns(ctx context.Context, tx *SQLTx, params map[string]interface{}, _ *ScanSpecs) (RowReader, error) {
	if len(stmt.fnCall.params) != 1 {
		return nil, fmt.Errorf("%w: function '%s' expect table name as parameter", ErrIllegalArguments, ColumnsFnCall)
	}

	cols := []ColDescriptor{
		{
			Column: "table",
			Type:   VarcharType,
		},
		{
			Column: "name",
			Type:   VarcharType,
		},
		{
			Column: "type",
			Type:   VarcharType,
		},
		{
			Column: "max_length",
			Type:   IntegerType,
		},
		{
			Column: "nullable",
			Type:   BooleanType,
		},
		{
			Column: "auto_increment",
			Type:   BooleanType,
		},
		{
			Column: "indexed",
			Type:   BooleanType,
		},
		{
			Column: "primary",
			Type:   BooleanType,
		},
		{
			Column: "unique",
			Type:   BooleanType,
		},
	}

	val, err := stmt.fnCall.params[0].substitute(params)
	if err != nil {
		return nil, err
	}

	tableName, err := val.reduce(tx, nil, "")
	if err != nil {
		return nil, err
	}

	if tableName.Type() != VarcharType {
		return nil, fmt.Errorf("%w: expected '%s' for table name but type '%s' given instead", ErrIllegalArguments, VarcharType, tableName.Type())
	}

	table, err := tx.catalog.GetTableByName(tableName.RawValue().(string))
	if err != nil {
		return nil, err
	}

	values := make([][]ValueExp, len(table.cols))

	for i, c := range table.cols {
		indexed, err := table.IsIndexed(c.Name())
		if err != nil {
			return nil, err
		}

		var unique bool
		for _, index := range table.indexesByColID[c.id] {
			if index.IsUnique() && len(index.Cols()) == 1 {
				unique = true
				break
			}
		}

		values[i] = []ValueExp{
			&Varchar{val: table.name},
			&Varchar{val: c.colName},
			&Varchar{val: c.colType},
			&Integer{val: int64(c.MaxLen())},
			&Bool{val: c.IsNullable()},
			&Bool{val: c.autoIncrement},
			&Bool{val: indexed},
			&Bool{val: table.PrimaryIndex().IncludesCol(c.ID())},
			&Bool{val: unique},
		}
	}

	return NewValuesRowReader(tx, params, cols, true, stmt.Alias(), values)
}

func (stmt *FnDataSourceStmt) resolveListIndexes(ctx context.Context, tx *SQLTx, params map[string]interface{}, _ *ScanSpecs) (RowReader, error) {
	if len(stmt.fnCall.params) != 1 {
		return nil, fmt.Errorf("%w: function '%s' expect table name as parameter", ErrIllegalArguments, IndexesFnCall)
	}

	cols := []ColDescriptor{
		{
			Column: "table",
			Type:   VarcharType,
		},
		{
			Column: "name",
			Type:   VarcharType,
		},
		{
			Column: "unique",
			Type:   BooleanType,
		},
		{
			Column: "primary",
			Type:   BooleanType,
		},
	}

	val, err := stmt.fnCall.params[0].substitute(params)
	if err != nil {
		return nil, err
	}

	tableName, err := val.reduce(tx, nil, "")
	if err != nil {
		return nil, err
	}

	if tableName.Type() != VarcharType {
		return nil, fmt.Errorf("%w: expected '%s' for table name but type '%s' given instead", ErrIllegalArguments, VarcharType, tableName.Type())
	}

	table, err := tx.catalog.GetTableByName(tableName.RawValue().(string))
	if err != nil {
		return nil, err
	}

	values := make([][]ValueExp, len(table.indexes))

	for i, index := range table.indexes {
		values[i] = []ValueExp{
			&Varchar{val: table.name},
			&Varchar{val: index.Name()},
			&Bool{val: index.unique},
			&Bool{val: index.IsPrimary()},
		}
	}

	return NewValuesRowReader(tx, params, cols, true, stmt.Alias(), values)
}

func (stmt *FnDataSourceStmt) resolveListGrants(ctx context.Context, tx *SQLTx, params map[string]interface{}, _ *ScanSpecs) (RowReader, error) {
	if len(stmt.fnCall.params) > 1 {
		return nil, fmt.Errorf("%w: function '%s' expect at most one parameter of type %s", ErrIllegalArguments, GrantsFnCall, VarcharType)
	}

	var username string
	if len(stmt.fnCall.params) == 1 {
		val, err := stmt.fnCall.params[0].substitute(params)
		if err != nil {
			return nil, err
		}

		userVal, err := val.reduce(tx, nil, "")
		if err != nil {
			return nil, err
		}

		if userVal.Type() != VarcharType {
			return nil, fmt.Errorf("%w: expected '%s' for username but type '%s' given instead", ErrIllegalArguments, VarcharType, userVal.Type())
		}
		username, _ = userVal.RawValue().(string)
	}

	cols := []ColDescriptor{
		{
			Column: "user",
			Type:   VarcharType,
		},
		{
			Column: "privilege",
			Type:   VarcharType,
		},
	}

	var err error
	var users []User

	if tx.engine.multidbHandler == nil {
		return nil, ErrUnspecifiedMultiDBHandler
	} else {
		users, err = tx.engine.multidbHandler.ListUsers(ctx)
		if err != nil {
			return nil, err
		}
	}

	values := make([][]ValueExp, 0, len(users))

	for _, user := range users {
		if username == "" || user.Username() == username {
			for _, p := range user.SQLPrivileges() {
				values = append(values, []ValueExp{
					&Varchar{val: user.Username()},
					&Varchar{val: string(p)},
				})
			}
		}
	}

	return NewValuesRowReader(tx, params, cols, true, stmt.Alias(), values)
}

// DropTableStmt represents a statement to delete a table.
type DropTableStmt struct {
	table string
}

func NewDropTableStmt(table string) *DropTableStmt {
	return &DropTableStmt{table: table}
}

func (stmt *DropTableStmt) readOnly() bool {
	return false
}

func (stmt *DropTableStmt) requiredPrivileges() []SQLPrivilege {
	return []SQLPrivilege{SQLPrivilegeDrop}
}

func (stmt *DropTableStmt) inferParameters(ctx context.Context, tx *SQLTx, params map[string]SQLValueType) error {
	return nil
}

/*
Exec executes the delete table statement.
It the table exists, if not it does nothing.
If the table exists, it deletes all the indexes and the table itself.
Note that this is a soft delete of the index and table key,
the data is not deleted, but the metadata is updated.
*/
func (stmt *DropTableStmt) execAt(ctx context.Context, tx *SQLTx, params map[string]interface{}) (*SQLTx, error) {
	if !tx.catalog.ExistTable(stmt.table) {
		return nil, ErrTableDoesNotExist
	}

	table, err := tx.catalog.GetTableByName(stmt.table)
	if err != nil {
		return nil, err
	}

	// delete table
	mappedKey := MapKey(
		tx.sqlPrefix(),
		catalogTablePrefix,
		EncodeID(DatabaseID),
		EncodeID(table.id),
	)
	err = tx.delete(ctx, mappedKey)
	if err != nil {
		return nil, err
	}

	// delete columns
	cols := table.ColumnsByID()
	for _, col := range cols {
		mappedKey := MapKey(
			tx.sqlPrefix(),
			catalogColumnPrefix,
			EncodeID(DatabaseID),
			EncodeID(col.table.id),
			EncodeID(col.id),
			[]byte(col.colType),
		)
		err = tx.delete(ctx, mappedKey)
		if err != nil {
			return nil, err
		}
	}

	// delete checks
	for name := range table.checkConstraints {
		key := MapKey(
			tx.sqlPrefix(),
			catalogCheckPrefix,
			EncodeID(DatabaseID),
			EncodeID(table.id),
			[]byte(name),
		)

		if err := tx.delete(ctx, key); err != nil {
			return nil, err
		}
	}

	// delete indexes
	for _, index := range table.indexes {
		mappedKey := MapKey(
			tx.sqlPrefix(),
			catalogIndexPrefix,
			EncodeID(DatabaseID),
			EncodeID(table.id),
			EncodeID(index.id),
		)
		err = tx.delete(ctx, mappedKey)
		if err != nil {
			return nil, err
		}

		indexKey := MapKey(
			tx.sqlPrefix(),
			MappedPrefix,
			EncodeID(table.id),
			EncodeID(index.id),
		)
		err = tx.addOnCommittedCallback(func(sqlTx *SQLTx) error {
			return sqlTx.engine.store.DeleteIndex(indexKey)
		})
		if err != nil {
			return nil, err
		}
	}

	err = tx.catalog.deleteTable(table)
	if err != nil {
		return nil, err
	}

	tx.mutatedCatalog = true

	return tx, nil
}

// TruncateTableStmt empties a table by dropping its catalog metadata and
// recreating it with the same schema under a fresh table ID. Old row keys
// remain on disk but are no longer reachable through the catalog, so the
// table appears empty. Unlike row-by-row DELETE this is O(schema), so it
// never hits the per-transaction entry limit.
type TruncateTableStmt struct {
	table string
}

func NewTruncateTableStmt(table string) *TruncateTableStmt {
	return &TruncateTableStmt{table: table}
}

func (stmt *TruncateTableStmt) readOnly() bool {
	return false
}

func (stmt *TruncateTableStmt) requiredPrivileges() []SQLPrivilege {
	return []SQLPrivilege{SQLPrivilegeDrop, SQLPrivilegeCreate}
}

func (stmt *TruncateTableStmt) inferParameters(ctx context.Context, tx *SQLTx, params map[string]SQLValueType) error {
	return nil
}

func (stmt *TruncateTableStmt) execAt(ctx context.Context, tx *SQLTx, params map[string]interface{}) (*SQLTx, error) {
	if !tx.catalog.ExistTable(stmt.table) {
		return nil, ErrTableDoesNotExist
	}

	table, err := tx.catalog.GetTableByName(stmt.table)
	if err != nil {
		return nil, err
	}

	// Capture schema before the drop mutates the in-memory catalog.
	origCols := table.ColumnsByID()
	colsSpec := make([]*ColSpec, 0, len(origCols))
	for _, c := range origCols {
		colsSpec = append(colsSpec, &ColSpec{
			colName:       c.colName,
			colType:       c.colType,
			maxLen:        c.maxLen,
			autoIncrement: c.autoIncrement,
			notNull:       c.notNull,
			defaultValue:  c.defaultValue,
		})
	}

	var pkColNames PrimaryKeyConstraint
	if table.primaryIndex != nil {
		for _, c := range table.primaryIndex.cols {
			pkColNames = append(pkColNames, c.colName)
		}
	}

	// Capture checks; we re-assign IDs via the create path.
	checks := make([]CheckConstraint, 0, len(table.checkConstraints))
	for _, cc := range table.checkConstraints {
		checks = append(checks, CheckConstraint{name: cc.name, exp: cc.exp})
	}

	// Capture non-primary indexes to recreate after the table exists again.
	type savedIndex struct {
		unique    bool
		colNames  []string
		predicate ValueExp
	}
	var savedIndexes []savedIndex
	for _, idx := range table.indexes {
		if idx == table.primaryIndex {
			continue
		}
		colNames := make([]string, 0, len(idx.cols))
		for _, c := range idx.cols {
			colNames = append(colNames, c.colName)
		}
		savedIndexes = append(savedIndexes, savedIndex{unique: idx.unique, colNames: colNames, predicate: idx.predicate})
	}

	// Drop the existing table (metadata-only, bounded work).
	drop := &DropTableStmt{table: stmt.table}
	if _, err := drop.execAt(ctx, tx, params); err != nil {
		return nil, err
	}

	// Recreate the table with the captured schema.
	create := &CreateTableStmt{
		table:      stmt.table,
		colsSpec:   colsSpec,
		checks:     checks,
		pkColNames: pkColNames,
	}
	if _, err := create.execAt(ctx, tx, params); err != nil {
		return nil, err
	}

	// Recreate secondary indexes.
	for _, si := range savedIndexes {
		idxStmt := &CreateIndexStmt{
			unique:    si.unique,
			table:     stmt.table,
			cols:      si.colNames,
			predicate: si.predicate,
		}
		if _, err := idxStmt.execAt(ctx, tx, params); err != nil {
			return nil, err
		}
	}

	return tx, nil
}

// DropIndexStmt represents a statement to delete a table.
type DropIndexStmt struct {
	table string
	cols  []string
}

func NewDropIndexStmt(table string, cols []string) *DropIndexStmt {
	return &DropIndexStmt{table: table, cols: cols}
}

func (stmt *DropIndexStmt) readOnly() bool {
	return false
}

func (stmt *DropIndexStmt) requiredPrivileges() []SQLPrivilege {
	return []SQLPrivilege{SQLPrivilegeDrop}
}

func (stmt *DropIndexStmt) inferParameters(ctx context.Context, tx *SQLTx, params map[string]SQLValueType) error {
	return nil
}

/*
Exec executes the delete index statement.
If the index exists, it deletes it. Note that this is a soft delete of the index
the data is not deleted, but the metadata is updated.
*/
func (stmt *DropIndexStmt) execAt(ctx context.Context, tx *SQLTx, params map[string]interface{}) (*SQLTx, error) {
	if !tx.catalog.ExistTable(stmt.table) {
		return nil, ErrTableDoesNotExist
	}

	table, err := tx.catalog.GetTableByName(stmt.table)
	if err != nil {
		return nil, err
	}

	cols := make([]*Column, len(stmt.cols))

	for i, colName := range stmt.cols {
		col, err := table.GetColumnByName(colName)
		if err != nil {
			return nil, err
		}

		cols[i] = col
	}

	index, err := table.GetIndexByName(indexName(table.name, cols))
	if err != nil {
		return nil, err
	}

	// delete index
	mappedKey := MapKey(
		tx.sqlPrefix(),
		catalogIndexPrefix,
		EncodeID(DatabaseID),
		EncodeID(table.id),
		EncodeID(index.id),
	)
	err = tx.delete(ctx, mappedKey)
	if err != nil {
		return nil, err
	}

	indexKey := MapKey(
		tx.sqlPrefix(),
		MappedPrefix,
		EncodeID(table.id),
		EncodeID(index.id),
	)

	err = tx.addOnCommittedCallback(func(sqlTx *SQLTx) error {
		return sqlTx.engine.store.DeleteIndex(indexKey)
	})
	if err != nil {
		return nil, err
	}

	err = table.deleteIndex(index)
	if err != nil {
		return nil, err
	}

	tx.mutatedCatalog = true

	return tx, nil
}

type SQLPrivilege string

const (
	SQLPrivilegeSelect SQLPrivilege = "SELECT"
	SQLPrivilegeCreate SQLPrivilege = "CREATE"
	SQLPrivilegeInsert SQLPrivilege = "INSERT"
	SQLPrivilegeUpdate SQLPrivilege = "UPDATE"
	SQLPrivilegeDelete SQLPrivilege = "DELETE"
	SQLPrivilegeDrop   SQLPrivilege = "DROP"
	SQLPrivilegeAlter  SQLPrivilege = "ALTER"
)

var allPrivileges = []SQLPrivilege{
	SQLPrivilegeSelect,
	SQLPrivilegeCreate,
	SQLPrivilegeInsert,
	SQLPrivilegeUpdate,
	SQLPrivilegeDelete,
	SQLPrivilegeDrop,
	SQLPrivilegeAlter,
}

func DefaultSQLPrivilegesForPermission(p Permission) []SQLPrivilege {
	switch p {
	case PermissionSysAdmin, PermissionAdmin, PermissionReadWrite:
		return allPrivileges
	case PermissionReadOnly:
		return []SQLPrivilege{SQLPrivilegeSelect}
	}
	return nil
}

type AlterPrivilegesStmt struct {
	database   string
	user       string
	privileges []SQLPrivilege
	isGrant    bool
}

func (stmt *AlterPrivilegesStmt) readOnly() bool {
	return false
}

func (stmt *AlterPrivilegesStmt) requiredPrivileges() []SQLPrivilege {
	return nil
}

func (stmt *AlterPrivilegesStmt) execAt(ctx context.Context, tx *SQLTx, params map[string]interface{}) (*SQLTx, error) {
	if tx.IsExplicitCloseRequired() {
		return nil, fmt.Errorf("%w: user privileges modification can not be done within a transaction", ErrNonTransactionalStmt)
	}

	if tx.engine.multidbHandler == nil {
		return nil, ErrUnspecifiedMultiDBHandler
	}

	var err error
	if stmt.isGrant {
		err = tx.engine.multidbHandler.GrantSQLPrivileges(ctx, stmt.database, stmt.user, stmt.privileges)
	} else {
		err = tx.engine.multidbHandler.RevokeSQLPrivileges(ctx, stmt.database, stmt.user, stmt.privileges)
	}
	return nil, err
}

func (stmt *AlterPrivilegesStmt) inferParameters(ctx context.Context, tx *SQLTx, params map[string]SQLValueType) error {
	return nil
}

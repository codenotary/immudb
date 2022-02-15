# Protocol Documentation
<a name="top"></a>

## Table of Contents

- [schema.proto](#schema.proto)
    - [AuthConfig](#immudb.schema.AuthConfig)
    - [ChangePasswordRequest](#immudb.schema.ChangePasswordRequest)
    - [ChangePermissionRequest](#immudb.schema.ChangePermissionRequest)
    - [Chunk](#immudb.schema.Chunk)
    - [Column](#immudb.schema.Column)
    - [CommittedSQLTx](#immudb.schema.CommittedSQLTx)
    - [CommittedSQLTx.FirstInsertedPKsEntry](#immudb.schema.CommittedSQLTx.FirstInsertedPKsEntry)
    - [CommittedSQLTx.LastInsertedPKsEntry](#immudb.schema.CommittedSQLTx.LastInsertedPKsEntry)
    - [CreateUserRequest](#immudb.schema.CreateUserRequest)
    - [Database](#immudb.schema.Database)
    - [DatabaseHealthResponse](#immudb.schema.DatabaseHealthResponse)
    - [DatabaseListResponse](#immudb.schema.DatabaseListResponse)
    - [DatabaseSettings](#immudb.schema.DatabaseSettings)
    - [DebugInfo](#immudb.schema.DebugInfo)
    - [DeleteKeysRequest](#immudb.schema.DeleteKeysRequest)
    - [DualProof](#immudb.schema.DualProof)
    - [Entries](#immudb.schema.Entries)
    - [Entry](#immudb.schema.Entry)
    - [EntryCount](#immudb.schema.EntryCount)
    - [ErrorInfo](#immudb.schema.ErrorInfo)
    - [ExecAllRequest](#immudb.schema.ExecAllRequest)
    - [Expiration](#immudb.schema.Expiration)
    - [HealthResponse](#immudb.schema.HealthResponse)
    - [HistoryRequest](#immudb.schema.HistoryRequest)
    - [ImmutableState](#immudb.schema.ImmutableState)
    - [InclusionProof](#immudb.schema.InclusionProof)
    - [IndexSettings](#immudb.schema.IndexSettings)
    - [KVMetadata](#immudb.schema.KVMetadata)
    - [Key](#immudb.schema.Key)
    - [KeyListRequest](#immudb.schema.KeyListRequest)
    - [KeyPrefix](#immudb.schema.KeyPrefix)
    - [KeyRequest](#immudb.schema.KeyRequest)
    - [KeyValue](#immudb.schema.KeyValue)
    - [LinearProof](#immudb.schema.LinearProof)
    - [LoginRequest](#immudb.schema.LoginRequest)
    - [LoginResponse](#immudb.schema.LoginResponse)
    - [MTLSConfig](#immudb.schema.MTLSConfig)
    - [NamedParam](#immudb.schema.NamedParam)
    - [NewTxRequest](#immudb.schema.NewTxRequest)
    - [NewTxResponse](#immudb.schema.NewTxResponse)
    - [Op](#immudb.schema.Op)
    - [OpenSessionRequest](#immudb.schema.OpenSessionRequest)
    - [OpenSessionResponse](#immudb.schema.OpenSessionResponse)
    - [Permission](#immudb.schema.Permission)
    - [Reference](#immudb.schema.Reference)
    - [ReferenceRequest](#immudb.schema.ReferenceRequest)
    - [RetryInfo](#immudb.schema.RetryInfo)
    - [Row](#immudb.schema.Row)
    - [SQLEntry](#immudb.schema.SQLEntry)
    - [SQLExecRequest](#immudb.schema.SQLExecRequest)
    - [SQLExecResult](#immudb.schema.SQLExecResult)
    - [SQLGetRequest](#immudb.schema.SQLGetRequest)
    - [SQLQueryRequest](#immudb.schema.SQLQueryRequest)
    - [SQLQueryResult](#immudb.schema.SQLQueryResult)
    - [SQLValue](#immudb.schema.SQLValue)
    - [ScanRequest](#immudb.schema.ScanRequest)
    - [Score](#immudb.schema.Score)
    - [SetActiveUserRequest](#immudb.schema.SetActiveUserRequest)
    - [SetRequest](#immudb.schema.SetRequest)
    - [Signature](#immudb.schema.Signature)
    - [Table](#immudb.schema.Table)
    - [Tx](#immudb.schema.Tx)
    - [TxEntry](#immudb.schema.TxEntry)
    - [TxHeader](#immudb.schema.TxHeader)
    - [TxList](#immudb.schema.TxList)
    - [TxMetadata](#immudb.schema.TxMetadata)
    - [TxRequest](#immudb.schema.TxRequest)
    - [TxScanRequest](#immudb.schema.TxScanRequest)
    - [UseDatabaseReply](#immudb.schema.UseDatabaseReply)
    - [UseSnapshotRequest](#immudb.schema.UseSnapshotRequest)
    - [User](#immudb.schema.User)
    - [UserList](#immudb.schema.UserList)
    - [UserRequest](#immudb.schema.UserRequest)
    - [VerifiableEntry](#immudb.schema.VerifiableEntry)
    - [VerifiableGetRequest](#immudb.schema.VerifiableGetRequest)
    - [VerifiableReferenceRequest](#immudb.schema.VerifiableReferenceRequest)
    - [VerifiableSQLEntry](#immudb.schema.VerifiableSQLEntry)
    - [VerifiableSQLEntry.ColIdsByNameEntry](#immudb.schema.VerifiableSQLEntry.ColIdsByNameEntry)
    - [VerifiableSQLEntry.ColLenByIdEntry](#immudb.schema.VerifiableSQLEntry.ColLenByIdEntry)
    - [VerifiableSQLEntry.ColNamesByIdEntry](#immudb.schema.VerifiableSQLEntry.ColNamesByIdEntry)
    - [VerifiableSQLEntry.ColTypesByIdEntry](#immudb.schema.VerifiableSQLEntry.ColTypesByIdEntry)
    - [VerifiableSQLGetRequest](#immudb.schema.VerifiableSQLGetRequest)
    - [VerifiableSetRequest](#immudb.schema.VerifiableSetRequest)
    - [VerifiableTx](#immudb.schema.VerifiableTx)
    - [VerifiableTxRequest](#immudb.schema.VerifiableTxRequest)
    - [VerifiableZAddRequest](#immudb.schema.VerifiableZAddRequest)
    - [ZAddRequest](#immudb.schema.ZAddRequest)
    - [ZEntries](#immudb.schema.ZEntries)
    - [ZEntry](#immudb.schema.ZEntry)
    - [ZScanRequest](#immudb.schema.ZScanRequest)
  
    - [PermissionAction](#immudb.schema.PermissionAction)
    - [TxMode](#immudb.schema.TxMode)
  
    - [ImmuService](#immudb.schema.ImmuService)
  
- [Scalar Value Types](#scalar-value-types)



<a name="schema.proto"></a>
<p align="right"><a href="#top">Top</a></p>

## schema.proto



<a name="immudb.schema.AuthConfig"></a>

### AuthConfig



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| kind | [uint32](#uint32) |  |  |






<a name="immudb.schema.ChangePasswordRequest"></a>

### ChangePasswordRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| user | [bytes](#bytes) |  |  |
| oldPassword | [bytes](#bytes) |  |  |
| newPassword | [bytes](#bytes) |  |  |






<a name="immudb.schema.ChangePermissionRequest"></a>

### ChangePermissionRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| action | [PermissionAction](#immudb.schema.PermissionAction) |  |  |
| username | [string](#string) |  |  |
| database | [string](#string) |  |  |
| permission | [uint32](#uint32) |  |  |






<a name="immudb.schema.Chunk"></a>

### Chunk



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| content | [bytes](#bytes) |  |  |






<a name="immudb.schema.Column"></a>

### Column



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| name | [string](#string) |  |  |
| type | [string](#string) |  |  |






<a name="immudb.schema.CommittedSQLTx"></a>

### CommittedSQLTx



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| header | [TxHeader](#immudb.schema.TxHeader) |  |  |
| updatedRows | [uint32](#uint32) |  |  |
| lastInsertedPKs | [CommittedSQLTx.LastInsertedPKsEntry](#immudb.schema.CommittedSQLTx.LastInsertedPKsEntry) | repeated |  |
| firstInsertedPKs | [CommittedSQLTx.FirstInsertedPKsEntry](#immudb.schema.CommittedSQLTx.FirstInsertedPKsEntry) | repeated |  |






<a name="immudb.schema.CommittedSQLTx.FirstInsertedPKsEntry"></a>

### CommittedSQLTx.FirstInsertedPKsEntry



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| key | [string](#string) |  |  |
| value | [SQLValue](#immudb.schema.SQLValue) |  |  |






<a name="immudb.schema.CommittedSQLTx.LastInsertedPKsEntry"></a>

### CommittedSQLTx.LastInsertedPKsEntry



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| key | [string](#string) |  |  |
| value | [SQLValue](#immudb.schema.SQLValue) |  |  |






<a name="immudb.schema.CreateUserRequest"></a>

### CreateUserRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| user | [bytes](#bytes) |  |  |
| password | [bytes](#bytes) |  |  |
| permission | [uint32](#uint32) |  |  |
| database | [string](#string) |  |  |






<a name="immudb.schema.Database"></a>

### Database



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| databaseName | [string](#string) |  |  |






<a name="immudb.schema.DatabaseHealthResponse"></a>

### DatabaseHealthResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| pendingRequests | [uint32](#uint32) |  |  |
| lastRequestCompletedAt | [int64](#int64) |  |  |






<a name="immudb.schema.DatabaseListResponse"></a>

### DatabaseListResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| databases | [Database](#immudb.schema.Database) | repeated |  |






<a name="immudb.schema.DatabaseSettings"></a>

### DatabaseSettings



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| databaseName | [string](#string) |  |  |
| replica | [bool](#bool) |  |  |
| masterDatabase | [string](#string) |  |  |
| masterAddress | [string](#string) |  |  |
| masterPort | [uint32](#uint32) |  |  |
| followerUsername | [string](#string) |  |  |
| followerPassword | [string](#string) |  |  |
| fileSize | [uint32](#uint32) |  |  |
| maxKeyLen | [uint32](#uint32) |  |  |
| maxValueLen | [uint32](#uint32) |  |  |
| maxTxEntries | [uint32](#uint32) |  |  |
| excludeCommitTime | [bool](#bool) |  |  |
| maxConcurrency | [uint32](#uint32) |  |  |
| maxIOConcurrency | [uint32](#uint32) |  |  |
| txLogCacheSize | [uint32](#uint32) |  |  |
| vLogMaxOpenedFiles | [uint32](#uint32) |  |  |
| txLogMaxOpenedFiles | [uint32](#uint32) |  |  |
| commitLogMaxOpenedFiles | [uint32](#uint32) |  |  |
| indexSettings | [IndexSettings](#immudb.schema.IndexSettings) |  |  |






<a name="immudb.schema.DebugInfo"></a>

### DebugInfo



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| stack | [string](#string) |  |  |






<a name="immudb.schema.DeleteKeysRequest"></a>

### DeleteKeysRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| keys | [bytes](#bytes) | repeated |  |
| sinceTx | [uint64](#uint64) |  |  |
| noWait | [bool](#bool) |  |  |






<a name="immudb.schema.DualProof"></a>

### DualProof



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| sourceTxHeader | [TxHeader](#immudb.schema.TxHeader) |  |  |
| targetTxHeader | [TxHeader](#immudb.schema.TxHeader) |  |  |
| inclusionProof | [bytes](#bytes) | repeated |  |
| consistencyProof | [bytes](#bytes) | repeated |  |
| targetBlTxAlh | [bytes](#bytes) |  |  |
| lastInclusionProof | [bytes](#bytes) | repeated |  |
| linearProof | [LinearProof](#immudb.schema.LinearProof) |  |  |






<a name="immudb.schema.Entries"></a>

### Entries



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| entries | [Entry](#immudb.schema.Entry) | repeated |  |






<a name="immudb.schema.Entry"></a>

### Entry



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| tx | [uint64](#uint64) |  |  |
| key | [bytes](#bytes) |  |  |
| value | [bytes](#bytes) |  |  |
| referencedBy | [Reference](#immudb.schema.Reference) |  |  |
| metadata | [KVMetadata](#immudb.schema.KVMetadata) |  |  |
| expired | [bool](#bool) |  |  |






<a name="immudb.schema.EntryCount"></a>

### EntryCount



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| count | [uint64](#uint64) |  |  |






<a name="immudb.schema.ErrorInfo"></a>

### ErrorInfo



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| code | [string](#string) |  |  |
| cause | [string](#string) |  |  |






<a name="immudb.schema.ExecAllRequest"></a>

### ExecAllRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| Operations | [Op](#immudb.schema.Op) | repeated |  |
| noWait | [bool](#bool) |  |  |






<a name="immudb.schema.Expiration"></a>

### Expiration



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| expiresAt | [int64](#int64) |  |  |






<a name="immudb.schema.HealthResponse"></a>

### HealthResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| status | [bool](#bool) |  |  |
| version | [string](#string) |  |  |






<a name="immudb.schema.HistoryRequest"></a>

### HistoryRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| key | [bytes](#bytes) |  |  |
| offset | [uint64](#uint64) |  |  |
| limit | [int32](#int32) |  |  |
| desc | [bool](#bool) |  |  |
| sinceTx | [uint64](#uint64) |  |  |






<a name="immudb.schema.ImmutableState"></a>

### ImmutableState



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| db | [string](#string) |  |  |
| txId | [uint64](#uint64) |  |  |
| txHash | [bytes](#bytes) |  |  |
| signature | [Signature](#immudb.schema.Signature) |  |  |






<a name="immudb.schema.InclusionProof"></a>

### InclusionProof



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| leaf | [int32](#int32) |  |  |
| width | [int32](#int32) |  |  |
| terms | [bytes](#bytes) | repeated |  |






<a name="immudb.schema.IndexSettings"></a>

### IndexSettings



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| synced | [bool](#bool) |  |  |
| flushThreshold | [uint32](#uint32) |  |  |
| syncThreshold | [uint32](#uint32) |  |  |
| cacheSize | [uint32](#uint32) |  |  |
| maxNodeSize | [uint32](#uint32) |  |  |
| maxActiveSnapshots | [uint32](#uint32) |  |  |
| renewSnapRootAfter | [uint64](#uint64) |  |  |
| compactionThld | [uint32](#uint32) |  |  |
| delayDuringCompaction | [uint32](#uint32) |  |  |
| nodesLogMaxOpenedFiles | [uint32](#uint32) |  |  |
| historyLogMaxOpenedFiles | [uint32](#uint32) |  |  |
| commitLogMaxOpenedFiles | [uint32](#uint32) |  |  |






<a name="immudb.schema.KVMetadata"></a>

### KVMetadata



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| deleted | [bool](#bool) |  |  |
| expiration | [Expiration](#immudb.schema.Expiration) |  |  |
| nonIndexeable | [bool](#bool) |  |  |






<a name="immudb.schema.Key"></a>

### Key



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| key | [bytes](#bytes) |  |  |






<a name="immudb.schema.KeyListRequest"></a>

### KeyListRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| keys | [bytes](#bytes) | repeated |  |
| sinceTx | [uint64](#uint64) |  |  |






<a name="immudb.schema.KeyPrefix"></a>

### KeyPrefix



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| prefix | [bytes](#bytes) |  |  |






<a name="immudb.schema.KeyRequest"></a>

### KeyRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| key | [bytes](#bytes) |  |  |
| atTx | [uint64](#uint64) |  |  |
| sinceTx | [uint64](#uint64) |  |  |
| noWait | [bool](#bool) |  |  |






<a name="immudb.schema.KeyValue"></a>

### KeyValue



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| key | [bytes](#bytes) |  |  |
| value | [bytes](#bytes) |  |  |
| metadata | [KVMetadata](#immudb.schema.KVMetadata) |  |  |






<a name="immudb.schema.LinearProof"></a>

### LinearProof



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| sourceTxId | [uint64](#uint64) |  |  |
| TargetTxId | [uint64](#uint64) |  |  |
| terms | [bytes](#bytes) | repeated |  |






<a name="immudb.schema.LoginRequest"></a>

### LoginRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| user | [bytes](#bytes) |  |  |
| password | [bytes](#bytes) |  |  |






<a name="immudb.schema.LoginResponse"></a>

### LoginResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| token | [string](#string) |  |  |
| warning | [bytes](#bytes) |  |  |






<a name="immudb.schema.MTLSConfig"></a>

### MTLSConfig



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| enabled | [bool](#bool) |  |  |






<a name="immudb.schema.NamedParam"></a>

### NamedParam



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| name | [string](#string) |  |  |
| value | [SQLValue](#immudb.schema.SQLValue) |  |  |






<a name="immudb.schema.NewTxRequest"></a>

### NewTxRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| mode | [TxMode](#immudb.schema.TxMode) |  |  |






<a name="immudb.schema.NewTxResponse"></a>

### NewTxResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| transactionID | [string](#string) |  |  |






<a name="immudb.schema.Op"></a>

### Op



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| kv | [KeyValue](#immudb.schema.KeyValue) |  |  |
| zAdd | [ZAddRequest](#immudb.schema.ZAddRequest) |  |  |
| ref | [ReferenceRequest](#immudb.schema.ReferenceRequest) |  |  |






<a name="immudb.schema.OpenSessionRequest"></a>

### OpenSessionRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| username | [bytes](#bytes) |  |  |
| password | [bytes](#bytes) |  |  |
| databaseName | [string](#string) |  |  |






<a name="immudb.schema.OpenSessionResponse"></a>

### OpenSessionResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| sessionID | [string](#string) |  |  |
| serverUUID | [string](#string) |  |  |






<a name="immudb.schema.Permission"></a>

### Permission



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| database | [string](#string) |  |  |
| permission | [uint32](#uint32) |  |  |






<a name="immudb.schema.Reference"></a>

### Reference



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| tx | [uint64](#uint64) |  |  |
| key | [bytes](#bytes) |  |  |
| atTx | [uint64](#uint64) |  |  |
| metadata | [KVMetadata](#immudb.schema.KVMetadata) |  |  |






<a name="immudb.schema.ReferenceRequest"></a>

### ReferenceRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| key | [bytes](#bytes) |  |  |
| referencedKey | [bytes](#bytes) |  |  |
| atTx | [uint64](#uint64) |  |  |
| boundRef | [bool](#bool) |  |  |
| noWait | [bool](#bool) |  |  |






<a name="immudb.schema.RetryInfo"></a>

### RetryInfo



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| retry_delay | [int32](#int32) |  |  |






<a name="immudb.schema.Row"></a>

### Row



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| columns | [string](#string) | repeated |  |
| values | [SQLValue](#immudb.schema.SQLValue) | repeated |  |






<a name="immudb.schema.SQLEntry"></a>

### SQLEntry



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| tx | [uint64](#uint64) |  |  |
| key | [bytes](#bytes) |  |  |
| value | [bytes](#bytes) |  |  |
| metadata | [KVMetadata](#immudb.schema.KVMetadata) |  |  |






<a name="immudb.schema.SQLExecRequest"></a>

### SQLExecRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| sql | [string](#string) |  |  |
| params | [NamedParam](#immudb.schema.NamedParam) | repeated |  |
| noWait | [bool](#bool) |  |  |






<a name="immudb.schema.SQLExecResult"></a>

### SQLExecResult



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| txs | [CommittedSQLTx](#immudb.schema.CommittedSQLTx) | repeated |  |
| ongoingTx | [bool](#bool) |  |  |






<a name="immudb.schema.SQLGetRequest"></a>

### SQLGetRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| table | [string](#string) |  |  |
| pkValues | [SQLValue](#immudb.schema.SQLValue) | repeated |  |
| atTx | [uint64](#uint64) |  |  |
| sinceTx | [uint64](#uint64) |  |  |






<a name="immudb.schema.SQLQueryRequest"></a>

### SQLQueryRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| sql | [string](#string) |  |  |
| params | [NamedParam](#immudb.schema.NamedParam) | repeated |  |
| reuseSnapshot | [bool](#bool) |  |  |






<a name="immudb.schema.SQLQueryResult"></a>

### SQLQueryResult



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| columns | [Column](#immudb.schema.Column) | repeated |  |
| rows | [Row](#immudb.schema.Row) | repeated |  |






<a name="immudb.schema.SQLValue"></a>

### SQLValue



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| null | [google.protobuf.NullValue](#google.protobuf.NullValue) |  |  |
| n | [int64](#int64) |  |  |
| s | [string](#string) |  |  |
| b | [bool](#bool) |  |  |
| bs | [bytes](#bytes) |  |  |
| ts | [int64](#int64) |  |  |






<a name="immudb.schema.ScanRequest"></a>

### ScanRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| seekKey | [bytes](#bytes) |  |  |
| prefix | [bytes](#bytes) |  |  |
| desc | [bool](#bool) |  |  |
| limit | [uint64](#uint64) |  |  |
| sinceTx | [uint64](#uint64) |  |  |
| noWait | [bool](#bool) |  |  |






<a name="immudb.schema.Score"></a>

### Score



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| score | [double](#double) |  |  |






<a name="immudb.schema.SetActiveUserRequest"></a>

### SetActiveUserRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| active | [bool](#bool) |  |  |
| username | [string](#string) |  |  |






<a name="immudb.schema.SetRequest"></a>

### SetRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| KVs | [KeyValue](#immudb.schema.KeyValue) | repeated |  |
| noWait | [bool](#bool) |  |  |






<a name="immudb.schema.Signature"></a>

### Signature



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| publicKey | [bytes](#bytes) |  |  |
| signature | [bytes](#bytes) |  |  |






<a name="immudb.schema.Table"></a>

### Table



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| tableName | [string](#string) |  |  |






<a name="immudb.schema.Tx"></a>

### Tx



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| header | [TxHeader](#immudb.schema.TxHeader) |  |  |
| entries | [TxEntry](#immudb.schema.TxEntry) | repeated |  |






<a name="immudb.schema.TxEntry"></a>

### TxEntry



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| key | [bytes](#bytes) |  |  |
| hValue | [bytes](#bytes) |  |  |
| vLen | [int32](#int32) |  |  |
| metadata | [KVMetadata](#immudb.schema.KVMetadata) |  |  |






<a name="immudb.schema.TxHeader"></a>

### TxHeader



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| id | [uint64](#uint64) |  |  |
| prevAlh | [bytes](#bytes) |  |  |
| ts | [int64](#int64) |  |  |
| nentries | [int32](#int32) |  |  |
| eH | [bytes](#bytes) |  |  |
| blTxId | [uint64](#uint64) |  |  |
| blRoot | [bytes](#bytes) |  |  |
| version | [int32](#int32) |  |  |
| metadata | [TxMetadata](#immudb.schema.TxMetadata) |  |  |






<a name="immudb.schema.TxList"></a>

### TxList



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| txs | [Tx](#immudb.schema.Tx) | repeated |  |






<a name="immudb.schema.TxMetadata"></a>

### TxMetadata







<a name="immudb.schema.TxRequest"></a>

### TxRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| tx | [uint64](#uint64) |  |  |






<a name="immudb.schema.TxScanRequest"></a>

### TxScanRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| initialTx | [uint64](#uint64) |  |  |
| limit | [uint32](#uint32) |  |  |
| desc | [bool](#bool) |  |  |






<a name="immudb.schema.UseDatabaseReply"></a>

### UseDatabaseReply



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| token | [string](#string) |  |  |






<a name="immudb.schema.UseSnapshotRequest"></a>

### UseSnapshotRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| sinceTx | [uint64](#uint64) |  |  |
| asBeforeTx | [uint64](#uint64) |  |  |






<a name="immudb.schema.User"></a>

### User



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| user | [bytes](#bytes) |  |  |
| permissions | [Permission](#immudb.schema.Permission) | repeated |  |
| createdby | [string](#string) |  |  |
| createdat | [string](#string) |  |  |
| active | [bool](#bool) |  |  |






<a name="immudb.schema.UserList"></a>

### UserList



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| users | [User](#immudb.schema.User) | repeated |  |






<a name="immudb.schema.UserRequest"></a>

### UserRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| user | [bytes](#bytes) |  |  |






<a name="immudb.schema.VerifiableEntry"></a>

### VerifiableEntry



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| entry | [Entry](#immudb.schema.Entry) |  |  |
| verifiableTx | [VerifiableTx](#immudb.schema.VerifiableTx) |  |  |
| inclusionProof | [InclusionProof](#immudb.schema.InclusionProof) |  |  |






<a name="immudb.schema.VerifiableGetRequest"></a>

### VerifiableGetRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| keyRequest | [KeyRequest](#immudb.schema.KeyRequest) |  |  |
| proveSinceTx | [uint64](#uint64) |  |  |






<a name="immudb.schema.VerifiableReferenceRequest"></a>

### VerifiableReferenceRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| referenceRequest | [ReferenceRequest](#immudb.schema.ReferenceRequest) |  |  |
| proveSinceTx | [uint64](#uint64) |  |  |






<a name="immudb.schema.VerifiableSQLEntry"></a>

### VerifiableSQLEntry



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| sqlEntry | [SQLEntry](#immudb.schema.SQLEntry) |  |  |
| verifiableTx | [VerifiableTx](#immudb.schema.VerifiableTx) |  |  |
| inclusionProof | [InclusionProof](#immudb.schema.InclusionProof) |  |  |
| DatabaseId | [uint32](#uint32) |  |  |
| TableId | [uint32](#uint32) |  |  |
| PKIDs | [uint32](#uint32) | repeated |  |
| ColNamesById | [VerifiableSQLEntry.ColNamesByIdEntry](#immudb.schema.VerifiableSQLEntry.ColNamesByIdEntry) | repeated |  |
| ColIdsByName | [VerifiableSQLEntry.ColIdsByNameEntry](#immudb.schema.VerifiableSQLEntry.ColIdsByNameEntry) | repeated |  |
| ColTypesById | [VerifiableSQLEntry.ColTypesByIdEntry](#immudb.schema.VerifiableSQLEntry.ColTypesByIdEntry) | repeated |  |
| ColLenById | [VerifiableSQLEntry.ColLenByIdEntry](#immudb.schema.VerifiableSQLEntry.ColLenByIdEntry) | repeated |  |






<a name="immudb.schema.VerifiableSQLEntry.ColIdsByNameEntry"></a>

### VerifiableSQLEntry.ColIdsByNameEntry



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| key | [string](#string) |  |  |
| value | [uint32](#uint32) |  |  |






<a name="immudb.schema.VerifiableSQLEntry.ColLenByIdEntry"></a>

### VerifiableSQLEntry.ColLenByIdEntry



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| key | [uint32](#uint32) |  |  |
| value | [int32](#int32) |  |  |






<a name="immudb.schema.VerifiableSQLEntry.ColNamesByIdEntry"></a>

### VerifiableSQLEntry.ColNamesByIdEntry



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| key | [uint32](#uint32) |  |  |
| value | [string](#string) |  |  |






<a name="immudb.schema.VerifiableSQLEntry.ColTypesByIdEntry"></a>

### VerifiableSQLEntry.ColTypesByIdEntry



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| key | [uint32](#uint32) |  |  |
| value | [string](#string) |  |  |






<a name="immudb.schema.VerifiableSQLGetRequest"></a>

### VerifiableSQLGetRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| sqlGetRequest | [SQLGetRequest](#immudb.schema.SQLGetRequest) |  |  |
| proveSinceTx | [uint64](#uint64) |  |  |






<a name="immudb.schema.VerifiableSetRequest"></a>

### VerifiableSetRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| setRequest | [SetRequest](#immudb.schema.SetRequest) |  |  |
| proveSinceTx | [uint64](#uint64) |  |  |






<a name="immudb.schema.VerifiableTx"></a>

### VerifiableTx



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| tx | [Tx](#immudb.schema.Tx) |  |  |
| dualProof | [DualProof](#immudb.schema.DualProof) |  |  |
| signature | [Signature](#immudb.schema.Signature) |  |  |






<a name="immudb.schema.VerifiableTxRequest"></a>

### VerifiableTxRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| tx | [uint64](#uint64) |  |  |
| proveSinceTx | [uint64](#uint64) |  |  |






<a name="immudb.schema.VerifiableZAddRequest"></a>

### VerifiableZAddRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| zAddRequest | [ZAddRequest](#immudb.schema.ZAddRequest) |  |  |
| proveSinceTx | [uint64](#uint64) |  |  |






<a name="immudb.schema.ZAddRequest"></a>

### ZAddRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| set | [bytes](#bytes) |  |  |
| score | [double](#double) |  |  |
| key | [bytes](#bytes) |  |  |
| atTx | [uint64](#uint64) |  |  |
| boundRef | [bool](#bool) |  |  |
| noWait | [bool](#bool) |  |  |






<a name="immudb.schema.ZEntries"></a>

### ZEntries



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| entries | [ZEntry](#immudb.schema.ZEntry) | repeated |  |






<a name="immudb.schema.ZEntry"></a>

### ZEntry



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| set | [bytes](#bytes) |  |  |
| key | [bytes](#bytes) |  |  |
| entry | [Entry](#immudb.schema.Entry) |  |  |
| score | [double](#double) |  |  |
| atTx | [uint64](#uint64) |  |  |






<a name="immudb.schema.ZScanRequest"></a>

### ZScanRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| set | [bytes](#bytes) |  |  |
| seekKey | [bytes](#bytes) |  |  |
| seekScore | [double](#double) |  |  |
| seekAtTx | [uint64](#uint64) |  |  |
| inclusiveSeek | [bool](#bool) |  |  |
| limit | [uint64](#uint64) |  |  |
| desc | [bool](#bool) |  |  |
| minScore | [Score](#immudb.schema.Score) |  |  |
| maxScore | [Score](#immudb.schema.Score) |  |  |
| sinceTx | [uint64](#uint64) |  |  |
| noWait | [bool](#bool) |  |  |





 


<a name="immudb.schema.PermissionAction"></a>

### PermissionAction


| Name | Number | Description |
| ---- | ------ | ----------- |
| GRANT | 0 |  |
| REVOKE | 1 |  |



<a name="immudb.schema.TxMode"></a>

### TxMode


| Name | Number | Description |
| ---- | ------ | ----------- |
| ReadOnly | 0 |  |
| WriteOnly | 1 |  |
| ReadWrite | 2 |  |


 

 


<a name="immudb.schema.ImmuService"></a>

### ImmuService
immudb gRPC &amp; REST service

| Method Name | Request Type | Response Type | Description |
| ----------- | ------------ | ------------- | ------------|
| ListUsers | [.google.protobuf.Empty](#google.protobuf.Empty) | [UserList](#immudb.schema.UserList) |  |
| CreateUser | [CreateUserRequest](#immudb.schema.CreateUserRequest) | [.google.protobuf.Empty](#google.protobuf.Empty) |  |
| ChangePassword | [ChangePasswordRequest](#immudb.schema.ChangePasswordRequest) | [.google.protobuf.Empty](#google.protobuf.Empty) |  |
| UpdateAuthConfig | [AuthConfig](#immudb.schema.AuthConfig) | [.google.protobuf.Empty](#google.protobuf.Empty) |  |
| UpdateMTLSConfig | [MTLSConfig](#immudb.schema.MTLSConfig) | [.google.protobuf.Empty](#google.protobuf.Empty) |  |
| OpenSession | [OpenSessionRequest](#immudb.schema.OpenSessionRequest) | [OpenSessionResponse](#immudb.schema.OpenSessionResponse) |  |
| CloseSession | [.google.protobuf.Empty](#google.protobuf.Empty) | [.google.protobuf.Empty](#google.protobuf.Empty) |  |
| KeepAlive | [.google.protobuf.Empty](#google.protobuf.Empty) | [.google.protobuf.Empty](#google.protobuf.Empty) |  |
| NewTx | [NewTxRequest](#immudb.schema.NewTxRequest) | [NewTxResponse](#immudb.schema.NewTxResponse) |  |
| Commit | [.google.protobuf.Empty](#google.protobuf.Empty) | [CommittedSQLTx](#immudb.schema.CommittedSQLTx) |  |
| Rollback | [.google.protobuf.Empty](#google.protobuf.Empty) | [.google.protobuf.Empty](#google.protobuf.Empty) |  |
| TxSQLExec | [SQLExecRequest](#immudb.schema.SQLExecRequest) | [.google.protobuf.Empty](#google.protobuf.Empty) |  |
| TxSQLQuery | [SQLQueryRequest](#immudb.schema.SQLQueryRequest) | [SQLQueryResult](#immudb.schema.SQLQueryResult) |  |
| Login | [LoginRequest](#immudb.schema.LoginRequest) | [LoginResponse](#immudb.schema.LoginResponse) |  |
| Logout | [.google.protobuf.Empty](#google.protobuf.Empty) | [.google.protobuf.Empty](#google.protobuf.Empty) |  |
| Set | [SetRequest](#immudb.schema.SetRequest) | [TxHeader](#immudb.schema.TxHeader) |  |
| VerifiableSet | [VerifiableSetRequest](#immudb.schema.VerifiableSetRequest) | [VerifiableTx](#immudb.schema.VerifiableTx) |  |
| Get | [KeyRequest](#immudb.schema.KeyRequest) | [Entry](#immudb.schema.Entry) |  |
| VerifiableGet | [VerifiableGetRequest](#immudb.schema.VerifiableGetRequest) | [VerifiableEntry](#immudb.schema.VerifiableEntry) |  |
| Delete | [DeleteKeysRequest](#immudb.schema.DeleteKeysRequest) | [TxHeader](#immudb.schema.TxHeader) |  |
| GetAll | [KeyListRequest](#immudb.schema.KeyListRequest) | [Entries](#immudb.schema.Entries) |  |
| ExecAll | [ExecAllRequest](#immudb.schema.ExecAllRequest) | [TxHeader](#immudb.schema.TxHeader) |  |
| Scan | [ScanRequest](#immudb.schema.ScanRequest) | [Entries](#immudb.schema.Entries) |  |
| Count | [KeyPrefix](#immudb.schema.KeyPrefix) | [EntryCount](#immudb.schema.EntryCount) | NOT YET SUPPORTED |
| CountAll | [.google.protobuf.Empty](#google.protobuf.Empty) | [EntryCount](#immudb.schema.EntryCount) | NOT YET SUPPORTED |
| TxById | [TxRequest](#immudb.schema.TxRequest) | [Tx](#immudb.schema.Tx) |  |
| VerifiableTxById | [VerifiableTxRequest](#immudb.schema.VerifiableTxRequest) | [VerifiableTx](#immudb.schema.VerifiableTx) |  |
| TxScan | [TxScanRequest](#immudb.schema.TxScanRequest) | [TxList](#immudb.schema.TxList) |  |
| History | [HistoryRequest](#immudb.schema.HistoryRequest) | [Entries](#immudb.schema.Entries) |  |
| Health | [.google.protobuf.Empty](#google.protobuf.Empty) | [HealthResponse](#immudb.schema.HealthResponse) |  |
| DatabaseHealth | [.google.protobuf.Empty](#google.protobuf.Empty) | [DatabaseHealthResponse](#immudb.schema.DatabaseHealthResponse) |  |
| CurrentState | [.google.protobuf.Empty](#google.protobuf.Empty) | [ImmutableState](#immudb.schema.ImmutableState) |  |
| SetReference | [ReferenceRequest](#immudb.schema.ReferenceRequest) | [TxHeader](#immudb.schema.TxHeader) |  |
| VerifiableSetReference | [VerifiableReferenceRequest](#immudb.schema.VerifiableReferenceRequest) | [VerifiableTx](#immudb.schema.VerifiableTx) |  |
| ZAdd | [ZAddRequest](#immudb.schema.ZAddRequest) | [TxHeader](#immudb.schema.TxHeader) |  |
| VerifiableZAdd | [VerifiableZAddRequest](#immudb.schema.VerifiableZAddRequest) | [VerifiableTx](#immudb.schema.VerifiableTx) |  |
| ZScan | [ZScanRequest](#immudb.schema.ZScanRequest) | [ZEntries](#immudb.schema.ZEntries) |  |
| CreateDatabase | [Database](#immudb.schema.Database) | [.google.protobuf.Empty](#google.protobuf.Empty) | DEPRECATED: kept for backward compatibility |
| CreateDatabaseWith | [DatabaseSettings](#immudb.schema.DatabaseSettings) | [.google.protobuf.Empty](#google.protobuf.Empty) |  |
| DatabaseList | [.google.protobuf.Empty](#google.protobuf.Empty) | [DatabaseListResponse](#immudb.schema.DatabaseListResponse) |  |
| UseDatabase | [Database](#immudb.schema.Database) | [UseDatabaseReply](#immudb.schema.UseDatabaseReply) |  |
| UpdateDatabase | [DatabaseSettings](#immudb.schema.DatabaseSettings) | [.google.protobuf.Empty](#google.protobuf.Empty) |  |
| GetDatabaseSettings | [.google.protobuf.Empty](#google.protobuf.Empty) | [DatabaseSettings](#immudb.schema.DatabaseSettings) |  |
| CompactIndex | [.google.protobuf.Empty](#google.protobuf.Empty) | [.google.protobuf.Empty](#google.protobuf.Empty) |  |
| ChangePermission | [ChangePermissionRequest](#immudb.schema.ChangePermissionRequest) | [.google.protobuf.Empty](#google.protobuf.Empty) |  |
| SetActiveUser | [SetActiveUserRequest](#immudb.schema.SetActiveUserRequest) | [.google.protobuf.Empty](#google.protobuf.Empty) |  |
| streamGet | [KeyRequest](#immudb.schema.KeyRequest) | [Chunk](#immudb.schema.Chunk) stream | Streams |
| streamSet | [Chunk](#immudb.schema.Chunk) stream | [TxHeader](#immudb.schema.TxHeader) |  |
| streamVerifiableGet | [VerifiableGetRequest](#immudb.schema.VerifiableGetRequest) | [Chunk](#immudb.schema.Chunk) stream |  |
| streamVerifiableSet | [Chunk](#immudb.schema.Chunk) stream | [VerifiableTx](#immudb.schema.VerifiableTx) |  |
| streamScan | [ScanRequest](#immudb.schema.ScanRequest) | [Chunk](#immudb.schema.Chunk) stream |  |
| streamZScan | [ZScanRequest](#immudb.schema.ZScanRequest) | [Chunk](#immudb.schema.Chunk) stream |  |
| streamHistory | [HistoryRequest](#immudb.schema.HistoryRequest) | [Chunk](#immudb.schema.Chunk) stream |  |
| streamExecAll | [Chunk](#immudb.schema.Chunk) stream | [TxHeader](#immudb.schema.TxHeader) |  |
| exportTx | [TxRequest](#immudb.schema.TxRequest) | [Chunk](#immudb.schema.Chunk) stream | Replication |
| replicateTx | [Chunk](#immudb.schema.Chunk) stream | [TxHeader](#immudb.schema.TxHeader) |  |
| SQLExec | [SQLExecRequest](#immudb.schema.SQLExecRequest) | [SQLExecResult](#immudb.schema.SQLExecResult) |  |
| SQLQuery | [SQLQueryRequest](#immudb.schema.SQLQueryRequest) | [SQLQueryResult](#immudb.schema.SQLQueryResult) |  |
| ListTables | [.google.protobuf.Empty](#google.protobuf.Empty) | [SQLQueryResult](#immudb.schema.SQLQueryResult) |  |
| DescribeTable | [Table](#immudb.schema.Table) | [SQLQueryResult](#immudb.schema.SQLQueryResult) |  |
| VerifiableSQLGet | [VerifiableSQLGetRequest](#immudb.schema.VerifiableSQLGetRequest) | [VerifiableSQLEntry](#immudb.schema.VerifiableSQLEntry) |  |

 



## Scalar Value Types

| .proto Type | Notes | C++ | Java | Python | Go | C# | PHP | Ruby |
| ----------- | ----- | --- | ---- | ------ | -- | -- | --- | ---- |
| <a name="double" /> double |  | double | double | float | float64 | double | float | Float |
| <a name="float" /> float |  | float | float | float | float32 | float | float | Float |
| <a name="int32" /> int32 | Uses variable-length encoding. Inefficient for encoding negative numbers – if your field is likely to have negative values, use sint32 instead. | int32 | int | int | int32 | int | integer | Bignum or Fixnum (as required) |
| <a name="int64" /> int64 | Uses variable-length encoding. Inefficient for encoding negative numbers – if your field is likely to have negative values, use sint64 instead. | int64 | long | int/long | int64 | long | integer/string | Bignum |
| <a name="uint32" /> uint32 | Uses variable-length encoding. | uint32 | int | int/long | uint32 | uint | integer | Bignum or Fixnum (as required) |
| <a name="uint64" /> uint64 | Uses variable-length encoding. | uint64 | long | int/long | uint64 | ulong | integer/string | Bignum or Fixnum (as required) |
| <a name="sint32" /> sint32 | Uses variable-length encoding. Signed int value. These more efficiently encode negative numbers than regular int32s. | int32 | int | int | int32 | int | integer | Bignum or Fixnum (as required) |
| <a name="sint64" /> sint64 | Uses variable-length encoding. Signed int value. These more efficiently encode negative numbers than regular int64s. | int64 | long | int/long | int64 | long | integer/string | Bignum |
| <a name="fixed32" /> fixed32 | Always four bytes. More efficient than uint32 if values are often greater than 2^28. | uint32 | int | int | uint32 | uint | integer | Bignum or Fixnum (as required) |
| <a name="fixed64" /> fixed64 | Always eight bytes. More efficient than uint64 if values are often greater than 2^56. | uint64 | long | int/long | uint64 | ulong | integer/string | Bignum |
| <a name="sfixed32" /> sfixed32 | Always four bytes. | int32 | int | int | int32 | int | integer | Bignum or Fixnum (as required) |
| <a name="sfixed64" /> sfixed64 | Always eight bytes. | int64 | long | int/long | int64 | long | integer/string | Bignum |
| <a name="bool" /> bool |  | bool | boolean | boolean | bool | bool | boolean | TrueClass/FalseClass |
| <a name="string" /> string | A string must always contain UTF-8 encoded or 7-bit ASCII text. | string | String | str/unicode | string | string | string | String (UTF-8) |
| <a name="bytes" /> bytes | May contain any arbitrary sequence of bytes. | string | ByteString | str | []byte | ByteString | string | String (ASCII-8BIT) |


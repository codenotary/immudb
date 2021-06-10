# Protocol Documentation
<a name="top"></a>

## Table of Contents

- [schema.proto](#schema.proto)
    - [AuthConfig](#immudb.schema.AuthConfig)
    - [ChangePasswordRequest](#immudb.schema.ChangePasswordRequest)
    - [ChangePermissionRequest](#immudb.schema.ChangePermissionRequest)
    - [Chunk](#immudb.schema.Chunk)
    - [Column](#immudb.schema.Column)
    - [CreateUserRequest](#immudb.schema.CreateUserRequest)
    - [Database](#immudb.schema.Database)
    - [DatabaseListResponse](#immudb.schema.DatabaseListResponse)
    - [DebugInfo](#immudb.schema.DebugInfo)
    - [DualProof](#immudb.schema.DualProof)
    - [Entries](#immudb.schema.Entries)
    - [Entry](#immudb.schema.Entry)
    - [EntryCount](#immudb.schema.EntryCount)
    - [ErrorInfo](#immudb.schema.ErrorInfo)
    - [ExecAllRequest](#immudb.schema.ExecAllRequest)
    - [HealthResponse](#immudb.schema.HealthResponse)
    - [HistoryRequest](#immudb.schema.HistoryRequest)
    - [ImmutableState](#immudb.schema.ImmutableState)
    - [InclusionProof](#immudb.schema.InclusionProof)
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
    - [Op](#immudb.schema.Op)
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
| replica | [bool](#bool) |  |  |
| srcDatabase | [string](#string) |  |  |
| srcAddress | [string](#string) |  |  |
| srcPort | [uint32](#uint32) |  |  |
| followerUsr | [string](#string) |  |  |
| followerPwd | [string](#string) |  |  |






<a name="immudb.schema.DatabaseListResponse"></a>

### DatabaseListResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| databases | [Database](#immudb.schema.Database) | repeated |  |






<a name="immudb.schema.DebugInfo"></a>

### DebugInfo



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| stack | [string](#string) |  |  |






<a name="immudb.schema.DualProof"></a>

### DualProof



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| sourceTxMetadata | [TxMetadata](#immudb.schema.TxMetadata) |  |  |
| targetTxMetadata | [TxMetadata](#immudb.schema.TxMetadata) |  |  |
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






<a name="immudb.schema.KeyValue"></a>

### KeyValue



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| key | [bytes](#bytes) |  |  |
| value | [bytes](#bytes) |  |  |






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






<a name="immudb.schema.Op"></a>

### Op



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| kv | [KeyValue](#immudb.schema.KeyValue) |  |  |
| zAdd | [ZAddRequest](#immudb.schema.ZAddRequest) |  |  |
| ref | [ReferenceRequest](#immudb.schema.ReferenceRequest) |  |  |






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
| ctxs | [TxMetadata](#immudb.schema.TxMetadata) | repeated |  |
| dtxs | [TxMetadata](#immudb.schema.TxMetadata) | repeated |  |






<a name="immudb.schema.SQLGetRequest"></a>

### SQLGetRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| table | [string](#string) |  |  |
| pkValue | [SQLValue](#immudb.schema.SQLValue) |  |  |
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
| n | [uint64](#uint64) |  |  |
| s | [string](#string) |  |  |
| b | [bool](#bool) |  |  |
| bs | [bytes](#bytes) |  |  |






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
| metadata | [TxMetadata](#immudb.schema.TxMetadata) |  |  |
| entries | [TxEntry](#immudb.schema.TxEntry) | repeated |  |






<a name="immudb.schema.TxEntry"></a>

### TxEntry



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| key | [bytes](#bytes) |  |  |
| hValue | [bytes](#bytes) |  |  |
| vLen | [int32](#int32) |  |  |






<a name="immudb.schema.TxList"></a>

### TxList



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| txs | [Tx](#immudb.schema.Tx) | repeated |  |






<a name="immudb.schema.TxMetadata"></a>

### TxMetadata



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| id | [uint64](#uint64) |  |  |
| prevAlh | [bytes](#bytes) |  |  |
| ts | [int64](#int64) |  |  |
| nentries | [int32](#int32) |  |  |
| eH | [bytes](#bytes) |  |  |
| blTxId | [uint64](#uint64) |  |  |
| blRoot | [bytes](#bytes) |  |  |






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
| DatabaseId | [uint64](#uint64) |  |  |
| TableId | [uint64](#uint64) |  |  |
| PKName | [string](#string) |  |  |
| ColNamesById | [VerifiableSQLEntry.ColNamesByIdEntry](#immudb.schema.VerifiableSQLEntry.ColNamesByIdEntry) | repeated |  |
| ColIdsByName | [VerifiableSQLEntry.ColIdsByNameEntry](#immudb.schema.VerifiableSQLEntry.ColIdsByNameEntry) | repeated |  |
| ColTypesById | [VerifiableSQLEntry.ColTypesByIdEntry](#immudb.schema.VerifiableSQLEntry.ColTypesByIdEntry) | repeated |  |






<a name="immudb.schema.VerifiableSQLEntry.ColIdsByNameEntry"></a>

### VerifiableSQLEntry.ColIdsByNameEntry



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| key | [string](#string) |  |  |
| value | [uint64](#uint64) |  |  |






<a name="immudb.schema.VerifiableSQLEntry.ColNamesByIdEntry"></a>

### VerifiableSQLEntry.ColNamesByIdEntry



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| key | [uint64](#uint64) |  |  |
| value | [string](#string) |  |  |






<a name="immudb.schema.VerifiableSQLEntry.ColTypesByIdEntry"></a>

### VerifiableSQLEntry.ColTypesByIdEntry



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| key | [uint64](#uint64) |  |  |
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


 

 


<a name="immudb.schema.ImmuService"></a>

### ImmuService
immudb gRPC &amp; REST service
IMPORTANT: All get and safeget functions return base64-encoded keys and values, while all set and safeset functions expect base64-encoded inputs.

| Method Name | Request Type | Response Type | Description |
| ----------- | ------------ | ------------- | ------------|
| ListUsers | [.google.protobuf.Empty](#google.protobuf.Empty) | [UserList](#immudb.schema.UserList) |  |
| CreateUser | [CreateUserRequest](#immudb.schema.CreateUserRequest) | [.google.protobuf.Empty](#google.protobuf.Empty) |  |
| ChangePassword | [ChangePasswordRequest](#immudb.schema.ChangePasswordRequest) | [.google.protobuf.Empty](#google.protobuf.Empty) |  |
| UpdateAuthConfig | [AuthConfig](#immudb.schema.AuthConfig) | [.google.protobuf.Empty](#google.protobuf.Empty) |  |
| UpdateMTLSConfig | [MTLSConfig](#immudb.schema.MTLSConfig) | [.google.protobuf.Empty](#google.protobuf.Empty) |  |
| Login | [LoginRequest](#immudb.schema.LoginRequest) | [LoginResponse](#immudb.schema.LoginResponse) |  |
| Logout | [.google.protobuf.Empty](#google.protobuf.Empty) | [.google.protobuf.Empty](#google.protobuf.Empty) |  |
| Set | [SetRequest](#immudb.schema.SetRequest) | [TxMetadata](#immudb.schema.TxMetadata) |  |
| VerifiableSet | [VerifiableSetRequest](#immudb.schema.VerifiableSetRequest) | [VerifiableTx](#immudb.schema.VerifiableTx) |  |
| Get | [KeyRequest](#immudb.schema.KeyRequest) | [Entry](#immudb.schema.Entry) |  |
| VerifiableGet | [VerifiableGetRequest](#immudb.schema.VerifiableGetRequest) | [VerifiableEntry](#immudb.schema.VerifiableEntry) |  |
| GetAll | [KeyListRequest](#immudb.schema.KeyListRequest) | [Entries](#immudb.schema.Entries) |  |
| ExecAll | [ExecAllRequest](#immudb.schema.ExecAllRequest) | [TxMetadata](#immudb.schema.TxMetadata) |  |
| Scan | [ScanRequest](#immudb.schema.ScanRequest) | [Entries](#immudb.schema.Entries) |  |
| Count | [KeyPrefix](#immudb.schema.KeyPrefix) | [EntryCount](#immudb.schema.EntryCount) |  |
| CountAll | [.google.protobuf.Empty](#google.protobuf.Empty) | [EntryCount](#immudb.schema.EntryCount) |  |
| TxById | [TxRequest](#immudb.schema.TxRequest) | [Tx](#immudb.schema.Tx) |  |
| VerifiableTxById | [VerifiableTxRequest](#immudb.schema.VerifiableTxRequest) | [VerifiableTx](#immudb.schema.VerifiableTx) |  |
| TxScan | [TxScanRequest](#immudb.schema.TxScanRequest) | [TxList](#immudb.schema.TxList) |  |
| History | [HistoryRequest](#immudb.schema.HistoryRequest) | [Entries](#immudb.schema.Entries) |  |
| Health | [.google.protobuf.Empty](#google.protobuf.Empty) | [HealthResponse](#immudb.schema.HealthResponse) |  |
| CurrentState | [.google.protobuf.Empty](#google.protobuf.Empty) | [ImmutableState](#immudb.schema.ImmutableState) |  |
| SetReference | [ReferenceRequest](#immudb.schema.ReferenceRequest) | [TxMetadata](#immudb.schema.TxMetadata) |  |
| VerifiableSetReference | [VerifiableReferenceRequest](#immudb.schema.VerifiableReferenceRequest) | [VerifiableTx](#immudb.schema.VerifiableTx) |  |
| ZAdd | [ZAddRequest](#immudb.schema.ZAddRequest) | [TxMetadata](#immudb.schema.TxMetadata) |  |
| VerifiableZAdd | [VerifiableZAddRequest](#immudb.schema.VerifiableZAddRequest) | [VerifiableTx](#immudb.schema.VerifiableTx) |  |
| ZScan | [ZScanRequest](#immudb.schema.ZScanRequest) | [ZEntries](#immudb.schema.ZEntries) |  |
| CreateDatabase | [Database](#immudb.schema.Database) | [.google.protobuf.Empty](#google.protobuf.Empty) |  |
| DatabaseList | [.google.protobuf.Empty](#google.protobuf.Empty) | [DatabaseListResponse](#immudb.schema.DatabaseListResponse) |  |
| UseDatabase | [Database](#immudb.schema.Database) | [UseDatabaseReply](#immudb.schema.UseDatabaseReply) |  |
| CleanIndex | [.google.protobuf.Empty](#google.protobuf.Empty) | [.google.protobuf.Empty](#google.protobuf.Empty) |  |
| ChangePermission | [ChangePermissionRequest](#immudb.schema.ChangePermissionRequest) | [.google.protobuf.Empty](#google.protobuf.Empty) |  |
| SetActiveUser | [SetActiveUserRequest](#immudb.schema.SetActiveUserRequest) | [.google.protobuf.Empty](#google.protobuf.Empty) |  |
| streamGet | [KeyRequest](#immudb.schema.KeyRequest) | [Chunk](#immudb.schema.Chunk) stream | Streams |
| streamSet | [Chunk](#immudb.schema.Chunk) stream | [TxMetadata](#immudb.schema.TxMetadata) |  |
| streamVerifiableGet | [VerifiableGetRequest](#immudb.schema.VerifiableGetRequest) | [Chunk](#immudb.schema.Chunk) stream |  |
| streamVerifiableSet | [Chunk](#immudb.schema.Chunk) stream | [VerifiableTx](#immudb.schema.VerifiableTx) |  |
| streamScan | [ScanRequest](#immudb.schema.ScanRequest) | [Chunk](#immudb.schema.Chunk) stream |  |
| streamZScan | [ZScanRequest](#immudb.schema.ZScanRequest) | [Chunk](#immudb.schema.Chunk) stream |  |
| streamHistory | [HistoryRequest](#immudb.schema.HistoryRequest) | [Chunk](#immudb.schema.Chunk) stream |  |
| streamExecAll | [Chunk](#immudb.schema.Chunk) stream | [TxMetadata](#immudb.schema.TxMetadata) |  |
| exportTx | [TxRequest](#immudb.schema.TxRequest) | [Chunk](#immudb.schema.Chunk) stream | Replicarion |
| replicateTx | [Chunk](#immudb.schema.Chunk) stream | [TxMetadata](#immudb.schema.TxMetadata) |  |
| UseSnapshot | [UseSnapshotRequest](#immudb.schema.UseSnapshotRequest) | [.google.protobuf.Empty](#google.protobuf.Empty) | SQL |
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


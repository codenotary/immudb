# Protocol Documentation
<a name="top"></a>

## Table of Contents

- [schema.proto](#schema.proto)
    - [AHTNullableSettings](#immudb.schema.AHTNullableSettings)
    - [AuthConfig](#immudb.schema.AuthConfig)
    - [ChangePasswordRequest](#immudb.schema.ChangePasswordRequest)
    - [ChangePermissionRequest](#immudb.schema.ChangePermissionRequest)
    - [ChangeSQLPrivilegesRequest](#immudb.schema.ChangeSQLPrivilegesRequest)
    - [ChangeSQLPrivilegesResponse](#immudb.schema.ChangeSQLPrivilegesResponse)
    - [Chunk](#immudb.schema.Chunk)
    - [Chunk.MetadataEntry](#immudb.schema.Chunk.MetadataEntry)
    - [Column](#immudb.schema.Column)
    - [CommittedSQLTx](#immudb.schema.CommittedSQLTx)
    - [CommittedSQLTx.FirstInsertedPKsEntry](#immudb.schema.CommittedSQLTx.FirstInsertedPKsEntry)
    - [CommittedSQLTx.LastInsertedPKsEntry](#immudb.schema.CommittedSQLTx.LastInsertedPKsEntry)
    - [CreateDatabaseRequest](#immudb.schema.CreateDatabaseRequest)
    - [CreateDatabaseResponse](#immudb.schema.CreateDatabaseResponse)
    - [CreateUserRequest](#immudb.schema.CreateUserRequest)
    - [Database](#immudb.schema.Database)
    - [DatabaseHealthResponse](#immudb.schema.DatabaseHealthResponse)
    - [DatabaseInfo](#immudb.schema.DatabaseInfo)
    - [DatabaseListRequestV2](#immudb.schema.DatabaseListRequestV2)
    - [DatabaseListResponse](#immudb.schema.DatabaseListResponse)
    - [DatabaseListResponseV2](#immudb.schema.DatabaseListResponseV2)
    - [DatabaseNullableSettings](#immudb.schema.DatabaseNullableSettings)
    - [DatabaseSettings](#immudb.schema.DatabaseSettings)
    - [DatabaseSettingsRequest](#immudb.schema.DatabaseSettingsRequest)
    - [DatabaseSettingsResponse](#immudb.schema.DatabaseSettingsResponse)
    - [DebugInfo](#immudb.schema.DebugInfo)
    - [DeleteDatabaseRequest](#immudb.schema.DeleteDatabaseRequest)
    - [DeleteDatabaseResponse](#immudb.schema.DeleteDatabaseResponse)
    - [DeleteKeysRequest](#immudb.schema.DeleteKeysRequest)
    - [DualProof](#immudb.schema.DualProof)
    - [DualProofV2](#immudb.schema.DualProofV2)
    - [Entries](#immudb.schema.Entries)
    - [EntriesSpec](#immudb.schema.EntriesSpec)
    - [Entry](#immudb.schema.Entry)
    - [EntryCount](#immudb.schema.EntryCount)
    - [EntryTypeSpec](#immudb.schema.EntryTypeSpec)
    - [ErrorInfo](#immudb.schema.ErrorInfo)
    - [ExecAllRequest](#immudb.schema.ExecAllRequest)
    - [Expiration](#immudb.schema.Expiration)
    - [ExportTxRequest](#immudb.schema.ExportTxRequest)
    - [FlushIndexRequest](#immudb.schema.FlushIndexRequest)
    - [FlushIndexResponse](#immudb.schema.FlushIndexResponse)
    - [HealthResponse](#immudb.schema.HealthResponse)
    - [HistoryRequest](#immudb.schema.HistoryRequest)
    - [ImmutableState](#immudb.schema.ImmutableState)
    - [InclusionProof](#immudb.schema.InclusionProof)
    - [IndexNullableSettings](#immudb.schema.IndexNullableSettings)
    - [KVMetadata](#immudb.schema.KVMetadata)
    - [Key](#immudb.schema.Key)
    - [KeyListRequest](#immudb.schema.KeyListRequest)
    - [KeyPrefix](#immudb.schema.KeyPrefix)
    - [KeyRequest](#immudb.schema.KeyRequest)
    - [KeyValue](#immudb.schema.KeyValue)
    - [LinearAdvanceProof](#immudb.schema.LinearAdvanceProof)
    - [LinearProof](#immudb.schema.LinearProof)
    - [LoadDatabaseRequest](#immudb.schema.LoadDatabaseRequest)
    - [LoadDatabaseResponse](#immudb.schema.LoadDatabaseResponse)
    - [LoginRequest](#immudb.schema.LoginRequest)
    - [LoginResponse](#immudb.schema.LoginResponse)
    - [MTLSConfig](#immudb.schema.MTLSConfig)
    - [NamedParam](#immudb.schema.NamedParam)
    - [NewTxRequest](#immudb.schema.NewTxRequest)
    - [NewTxResponse](#immudb.schema.NewTxResponse)
    - [NullableBool](#immudb.schema.NullableBool)
    - [NullableFloat](#immudb.schema.NullableFloat)
    - [NullableMilliseconds](#immudb.schema.NullableMilliseconds)
    - [NullableString](#immudb.schema.NullableString)
    - [NullableUint32](#immudb.schema.NullableUint32)
    - [NullableUint64](#immudb.schema.NullableUint64)
    - [Op](#immudb.schema.Op)
    - [OpenSessionRequest](#immudb.schema.OpenSessionRequest)
    - [OpenSessionResponse](#immudb.schema.OpenSessionResponse)
    - [Permission](#immudb.schema.Permission)
    - [Precondition](#immudb.schema.Precondition)
    - [Precondition.KeyMustExistPrecondition](#immudb.schema.Precondition.KeyMustExistPrecondition)
    - [Precondition.KeyMustNotExistPrecondition](#immudb.schema.Precondition.KeyMustNotExistPrecondition)
    - [Precondition.KeyNotModifiedAfterTXPrecondition](#immudb.schema.Precondition.KeyNotModifiedAfterTXPrecondition)
    - [Reference](#immudb.schema.Reference)
    - [ReferenceRequest](#immudb.schema.ReferenceRequest)
    - [ReplicaState](#immudb.schema.ReplicaState)
    - [ReplicationNullableSettings](#immudb.schema.ReplicationNullableSettings)
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
    - [ServerInfoRequest](#immudb.schema.ServerInfoRequest)
    - [ServerInfoResponse](#immudb.schema.ServerInfoResponse)
    - [SetActiveUserRequest](#immudb.schema.SetActiveUserRequest)
    - [SetRequest](#immudb.schema.SetRequest)
    - [Signature](#immudb.schema.Signature)
    - [Table](#immudb.schema.Table)
    - [TruncateDatabaseRequest](#immudb.schema.TruncateDatabaseRequest)
    - [TruncateDatabaseResponse](#immudb.schema.TruncateDatabaseResponse)
    - [TruncationNullableSettings](#immudb.schema.TruncationNullableSettings)
    - [Tx](#immudb.schema.Tx)
    - [TxEntry](#immudb.schema.TxEntry)
    - [TxHeader](#immudb.schema.TxHeader)
    - [TxList](#immudb.schema.TxList)
    - [TxMetadata](#immudb.schema.TxMetadata)
    - [TxRequest](#immudb.schema.TxRequest)
    - [TxScanRequest](#immudb.schema.TxScanRequest)
    - [UnloadDatabaseRequest](#immudb.schema.UnloadDatabaseRequest)
    - [UnloadDatabaseResponse](#immudb.schema.UnloadDatabaseResponse)
    - [UpdateDatabaseRequest](#immudb.schema.UpdateDatabaseRequest)
    - [UpdateDatabaseResponse](#immudb.schema.UpdateDatabaseResponse)
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
    - [VerifiableTxV2](#immudb.schema.VerifiableTxV2)
    - [VerifiableZAddRequest](#immudb.schema.VerifiableZAddRequest)
    - [ZAddRequest](#immudb.schema.ZAddRequest)
    - [ZEntries](#immudb.schema.ZEntries)
    - [ZEntry](#immudb.schema.ZEntry)
    - [ZScanRequest](#immudb.schema.ZScanRequest)
  
    - [EntryTypeAction](#immudb.schema.EntryTypeAction)
    - [PermissionAction](#immudb.schema.PermissionAction)
    - [SQLPrivilege](#immudb.schema.SQLPrivilege)
    - [TxMode](#immudb.schema.TxMode)
  
    - [ImmuService](#immudb.schema.ImmuService)
  
- [Scalar Value Types](#scalar-value-types)



<a name="schema.proto"></a>
<p align="right"><a href="#top">Top</a></p>

## schema.proto



<a name="immudb.schema.AHTNullableSettings"></a>

### AHTNullableSettings



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| syncThreshold | [NullableUint32](#immudb.schema.NullableUint32) |  | Number of new leaves in the tree between synchronous flush to disk |
| writeBufferSize | [NullableUint32](#immudb.schema.NullableUint32) |  | Size of the in-memory write buffer |






<a name="immudb.schema.AuthConfig"></a>

### AuthConfig
DEPRECATED


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| kind | [uint32](#uint32) |  |  |






<a name="immudb.schema.ChangePasswordRequest"></a>

### ChangePasswordRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| user | [bytes](#bytes) |  | Username |
| oldPassword | [bytes](#bytes) |  | Old password |
| newPassword | [bytes](#bytes) |  | New password |






<a name="immudb.schema.ChangePermissionRequest"></a>

### ChangePermissionRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| action | [PermissionAction](#immudb.schema.PermissionAction) |  | Action to perform |
| username | [string](#string) |  | Name of the user to update |
| database | [string](#string) |  | Name of the database |
| permission | [uint32](#uint32) |  | Permission to grant / revoke: 1 - read only, 2 - read/write, 254 - admin |






<a name="immudb.schema.ChangeSQLPrivilegesRequest"></a>

### ChangeSQLPrivilegesRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| action | [PermissionAction](#immudb.schema.PermissionAction) |  | Action to perform |
| username | [string](#string) |  | Name of the user to update |
| database | [string](#string) |  | Name of the database |
| privileges | [SQLPrivilege](#immudb.schema.SQLPrivilege) | repeated | SQL privileges to grant / revoke |






<a name="immudb.schema.ChangeSQLPrivilegesResponse"></a>

### ChangeSQLPrivilegesResponse







<a name="immudb.schema.Chunk"></a>

### Chunk



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| content | [bytes](#bytes) |  |  |
| metadata | [Chunk.MetadataEntry](#immudb.schema.Chunk.MetadataEntry) | repeated |  |






<a name="immudb.schema.Chunk.MetadataEntry"></a>

### Chunk.MetadataEntry



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| key | [string](#string) |  |  |
| value | [bytes](#bytes) |  |  |






<a name="immudb.schema.Column"></a>

### Column



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| name | [string](#string) |  | Column name |
| type | [string](#string) |  | Column type |






<a name="immudb.schema.CommittedSQLTx"></a>

### CommittedSQLTx



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| header | [TxHeader](#immudb.schema.TxHeader) |  | Transaction header |
| updatedRows | [uint32](#uint32) |  | Number of updated rows |
| lastInsertedPKs | [CommittedSQLTx.LastInsertedPKsEntry](#immudb.schema.CommittedSQLTx.LastInsertedPKsEntry) | repeated | The value of last inserted auto_increment primary key (mapped by table name) |
| firstInsertedPKs | [CommittedSQLTx.FirstInsertedPKsEntry](#immudb.schema.CommittedSQLTx.FirstInsertedPKsEntry) | repeated | The value of first inserted auto_increment primary key (mapped by table name) |






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






<a name="immudb.schema.CreateDatabaseRequest"></a>

### CreateDatabaseRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| name | [string](#string) |  | Database name |
| settings | [DatabaseNullableSettings](#immudb.schema.DatabaseNullableSettings) |  | Database settings |
| ifNotExists | [bool](#bool) |  | If set to true, do not fail if the database already exists |






<a name="immudb.schema.CreateDatabaseResponse"></a>

### CreateDatabaseResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| name | [string](#string) |  | Database name |
| settings | [DatabaseNullableSettings](#immudb.schema.DatabaseNullableSettings) |  | Current database settings |
| alreadyExisted | [bool](#bool) |  | Set to true if given database already existed |






<a name="immudb.schema.CreateUserRequest"></a>

### CreateUserRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| user | [bytes](#bytes) |  | Username |
| password | [bytes](#bytes) |  | Login password |
| permission | [uint32](#uint32) |  | Permission, 1 - read permission, 2 - read&#43;write permission, 254 - admin |
| database | [string](#string) |  | Database name |






<a name="immudb.schema.Database"></a>

### Database



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| databaseName | [string](#string) |  | Name of the database |






<a name="immudb.schema.DatabaseHealthResponse"></a>

### DatabaseHealthResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| pendingRequests | [uint32](#uint32) |  | Number of requests currently being executed |
| lastRequestCompletedAt | [int64](#int64) |  | Timestamp at which the last request was completed |






<a name="immudb.schema.DatabaseInfo"></a>

### DatabaseInfo



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| name | [string](#string) |  | Database name |
| settings | [DatabaseNullableSettings](#immudb.schema.DatabaseNullableSettings) |  | Current database settings |
| loaded | [bool](#bool) |  | If true, this database is currently loaded into memory |
| diskSize | [uint64](#uint64) |  | database disk size |
| numTransactions | [uint64](#uint64) |  | total number of transactions |
| created_at | [uint64](#uint64) |  | the time when the db was created |
| created_by | [string](#string) |  | the user who created the database |






<a name="immudb.schema.DatabaseListRequestV2"></a>

### DatabaseListRequestV2







<a name="immudb.schema.DatabaseListResponse"></a>

### DatabaseListResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| databases | [Database](#immudb.schema.Database) | repeated | Database list |






<a name="immudb.schema.DatabaseListResponseV2"></a>

### DatabaseListResponseV2



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| databases | [DatabaseInfo](#immudb.schema.DatabaseInfo) | repeated | Database list with current database settings |






<a name="immudb.schema.DatabaseNullableSettings"></a>

### DatabaseNullableSettings



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| replicationSettings | [ReplicationNullableSettings](#immudb.schema.ReplicationNullableSettings) |  | Replication settings |
| fileSize | [NullableUint32](#immudb.schema.NullableUint32) |  | Max filesize on disk |
| maxKeyLen | [NullableUint32](#immudb.schema.NullableUint32) |  | Maximum length of keys |
| maxValueLen | [NullableUint32](#immudb.schema.NullableUint32) |  | Maximum length of values |
| maxTxEntries | [NullableUint32](#immudb.schema.NullableUint32) |  | Maximum number of entries in a single transaction |
| excludeCommitTime | [NullableBool](#immudb.schema.NullableBool) |  | If set to true, do not include commit timestamp in transaction headers |
| maxConcurrency | [NullableUint32](#immudb.schema.NullableUint32) |  | Maximum number of simultaneous commits prepared for write |
| maxIOConcurrency | [NullableUint32](#immudb.schema.NullableUint32) |  | Maximum number of simultaneous IO writes |
| txLogCacheSize | [NullableUint32](#immudb.schema.NullableUint32) |  | Size of the cache for transaction logs |
| vLogMaxOpenedFiles | [NullableUint32](#immudb.schema.NullableUint32) |  | Maximum number of simultaneous value files opened |
| txLogMaxOpenedFiles | [NullableUint32](#immudb.schema.NullableUint32) |  | Maximum number of simultaneous transaction log files opened |
| commitLogMaxOpenedFiles | [NullableUint32](#immudb.schema.NullableUint32) |  | Maximum number of simultaneous commit log files opened |
| indexSettings | [IndexNullableSettings](#immudb.schema.IndexNullableSettings) |  | Index settings |
| writeTxHeaderVersion | [NullableUint32](#immudb.schema.NullableUint32) |  | Version of transaction header to use (limits available features) |
| autoload | [NullableBool](#immudb.schema.NullableBool) |  | If set to true, automatically load the database when starting immudb (true by default) |
| readTxPoolSize | [NullableUint32](#immudb.schema.NullableUint32) |  | Size of the pool of read buffers |
| syncFrequency | [NullableMilliseconds](#immudb.schema.NullableMilliseconds) |  | Fsync frequency during commit process |
| writeBufferSize | [NullableUint32](#immudb.schema.NullableUint32) |  | Size of the in-memory buffer for write operations |
| ahtSettings | [AHTNullableSettings](#immudb.schema.AHTNullableSettings) |  | Settings of Appendable Hash Tree |
| maxActiveTransactions | [NullableUint32](#immudb.schema.NullableUint32) |  | Maximum number of pre-committed transactions |
| mvccReadSetLimit | [NullableUint32](#immudb.schema.NullableUint32) |  | Limit the number of read entries per transaction |
| vLogCacheSize | [NullableUint32](#immudb.schema.NullableUint32) |  | Size of the cache for value logs |
| truncationSettings | [TruncationNullableSettings](#immudb.schema.TruncationNullableSettings) |  | Truncation settings |
| embeddedValues | [NullableBool](#immudb.schema.NullableBool) |  | If set to true, values are stored together with the transaction header (true by default) |
| preallocFiles | [NullableBool](#immudb.schema.NullableBool) |  | Enable file preallocation |






<a name="immudb.schema.DatabaseSettings"></a>

### DatabaseSettings



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| databaseName | [string](#string) |  | Name of the database |
| replica | [bool](#bool) |  | If set to true, this database is replicating another database |
| primaryDatabase | [string](#string) |  | Name of the database to replicate |
| primaryHost | [string](#string) |  | Hostname of the immudb instance with database to replicate |
| primaryPort | [uint32](#uint32) |  | Port of the immudb instance with database to replicate |
| primaryUsername | [string](#string) |  | Username of the user with read access of the database to replicate |
| primaryPassword | [string](#string) |  | Password of the user with read access of the database to replicate |
| fileSize | [uint32](#uint32) |  | Size of files stored on disk |
| maxKeyLen | [uint32](#uint32) |  | Maximum length of keys |
| maxValueLen | [uint32](#uint32) |  | Maximum length of values |
| maxTxEntries | [uint32](#uint32) |  | Maximum number of entries in a single transaction |
| excludeCommitTime | [bool](#bool) |  | If set to true, do not include commit timestamp in transaction headers |






<a name="immudb.schema.DatabaseSettingsRequest"></a>

### DatabaseSettingsRequest







<a name="immudb.schema.DatabaseSettingsResponse"></a>

### DatabaseSettingsResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| database | [string](#string) |  | Database name |
| settings | [DatabaseNullableSettings](#immudb.schema.DatabaseNullableSettings) |  | Database settings |






<a name="immudb.schema.DebugInfo"></a>

### DebugInfo



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| stack | [string](#string) |  | Stack trace when the error was noticed |






<a name="immudb.schema.DeleteDatabaseRequest"></a>

### DeleteDatabaseRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| database | [string](#string) |  | Database name |






<a name="immudb.schema.DeleteDatabaseResponse"></a>

### DeleteDatabaseResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| database | [string](#string) |  | Database name |






<a name="immudb.schema.DeleteKeysRequest"></a>

### DeleteKeysRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| keys | [bytes](#bytes) | repeated | List of keys to delete logically |
| sinceTx | [uint64](#uint64) |  | If 0, wait for index to be up-to-date, If &gt; 0, wait for at least sinceTx transaction to be indexed |
| noWait | [bool](#bool) |  | If set to true, do not wait for the indexer to index this operation |






<a name="immudb.schema.DualProof"></a>

### DualProof
DualProof contains inclusion and consistency proofs for dual Merkle-Tree &#43; Linear proofs


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| sourceTxHeader | [TxHeader](#immudb.schema.TxHeader) |  | Header of the source (earlier) transaction |
| targetTxHeader | [TxHeader](#immudb.schema.TxHeader) |  | Header of the target (latter) transaction |
| inclusionProof | [bytes](#bytes) | repeated | Inclusion proof of the source transaction hash in the main Merkle Tree |
| consistencyProof | [bytes](#bytes) | repeated | Consistency proof between Merkle Trees in the source and target transactions |
| targetBlTxAlh | [bytes](#bytes) |  | Accumulative hash (Alh) of the last transaction that&#39;s part of the target Merkle Tree |
| lastInclusionProof | [bytes](#bytes) | repeated | Inclusion proof of the targetBlTxAlh in the target Merkle Tree |
| linearProof | [LinearProof](#immudb.schema.LinearProof) |  | Linear proof starting from targetBlTxAlh to the final state value |
| LinearAdvanceProof | [LinearAdvanceProof](#immudb.schema.LinearAdvanceProof) |  | Proof of consistency between some part of older linear chain and newer Merkle Tree |






<a name="immudb.schema.DualProofV2"></a>

### DualProofV2
DualProofV2 contains inclusion and consistency proofs


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| sourceTxHeader | [TxHeader](#immudb.schema.TxHeader) |  | Header of the source (earlier) transaction |
| targetTxHeader | [TxHeader](#immudb.schema.TxHeader) |  | Header of the target (latter) transaction |
| inclusionProof | [bytes](#bytes) | repeated | Inclusion proof of the source transaction hash in the main Merkle Tree |
| consistencyProof | [bytes](#bytes) | repeated | Consistency proof between Merkle Trees in the source and target transactions |






<a name="immudb.schema.Entries"></a>

### Entries



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| entries | [Entry](#immudb.schema.Entry) | repeated | List of entries |






<a name="immudb.schema.EntriesSpec"></a>

### EntriesSpec



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| kvEntriesSpec | [EntryTypeSpec](#immudb.schema.EntryTypeSpec) |  | Specification for parsing KV entries |
| zEntriesSpec | [EntryTypeSpec](#immudb.schema.EntryTypeSpec) |  | Specification for parsing sorted set entries |
| sqlEntriesSpec | [EntryTypeSpec](#immudb.schema.EntryTypeSpec) |  | Specification for parsing SQL entries |






<a name="immudb.schema.Entry"></a>

### Entry



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| tx | [uint64](#uint64) |  | Transaction id at which the target value was set (i.e. not the reference transaction id) |
| key | [bytes](#bytes) |  | Key of the target value (i.e. not the reference entry) |
| value | [bytes](#bytes) |  | Value |
| referencedBy | [Reference](#immudb.schema.Reference) |  | If the request was for a reference, this field will keep information about the reference entry |
| metadata | [KVMetadata](#immudb.schema.KVMetadata) |  | Metadata of the target entry (i.e. not the reference entry) |
| expired | [bool](#bool) |  | If set to true, this entry has expired and the value is not retrieved |
| revision | [uint64](#uint64) |  | Key&#39;s revision, in case of GetAt it will be 0 |






<a name="immudb.schema.EntryCount"></a>

### EntryCount



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| count | [uint64](#uint64) |  |  |






<a name="immudb.schema.EntryTypeSpec"></a>

### EntryTypeSpec



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| action | [EntryTypeAction](#immudb.schema.EntryTypeAction) |  | Action to perform on entries |






<a name="immudb.schema.ErrorInfo"></a>

### ErrorInfo



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| code | [string](#string) |  | Error code |
| cause | [string](#string) |  | Error Description |






<a name="immudb.schema.ExecAllRequest"></a>

### ExecAllRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| Operations | [Op](#immudb.schema.Op) | repeated | List of operations to perform |
| noWait | [bool](#bool) |  | If set to true, do not wait for indexing to process this transaction |
| preconditions | [Precondition](#immudb.schema.Precondition) | repeated | Preconditions to check |






<a name="immudb.schema.Expiration"></a>

### Expiration



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| expiresAt | [int64](#int64) |  | Entry expiration time (unix timestamp in seconds) |






<a name="immudb.schema.ExportTxRequest"></a>

### ExportTxRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| tx | [uint64](#uint64) |  | Id of transaction to export |
| allowPreCommitted | [bool](#bool) |  | If set to true, non-committed transactions can be exported |
| replicaState | [ReplicaState](#immudb.schema.ReplicaState) |  | Used on synchronous replication to notify the primary about replica state |
| skipIntegrityCheck | [bool](#bool) |  | If set to true, integrity checks are skipped when reading data |






<a name="immudb.schema.FlushIndexRequest"></a>

### FlushIndexRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| cleanupPercentage | [float](#float) |  | Percentage of nodes file to cleanup during flush |
| synced | [bool](#bool) |  | If true, do a full disk sync after the flush |






<a name="immudb.schema.FlushIndexResponse"></a>

### FlushIndexResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| database | [string](#string) |  | Database name |






<a name="immudb.schema.HealthResponse"></a>

### HealthResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| status | [bool](#bool) |  | If true, server considers itself to be healthy |
| version | [string](#string) |  | The version of the server instance |






<a name="immudb.schema.HistoryRequest"></a>

### HistoryRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| key | [bytes](#bytes) |  | Name of the key to query for the history |
| offset | [uint64](#uint64) |  | Specify the initial entry to be returned by excluding the initial set of entries |
| limit | [int32](#int32) |  | Maximum number of entries to return |
| desc | [bool](#bool) |  | If true, search in descending order |
| sinceTx | [uint64](#uint64) |  | If &gt; 0, do not wait for the indexer to index all entries, only require entries up to sinceTx to be indexed |






<a name="immudb.schema.ImmutableState"></a>

### ImmutableState



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| db | [string](#string) |  | The db name |
| txId | [uint64](#uint64) |  | Id of the most recent transaction |
| txHash | [bytes](#bytes) |  | State of the most recent transaction |
| signature | [Signature](#immudb.schema.Signature) |  | Signature of the hash |
| precommittedTxId | [uint64](#uint64) |  | Id of the most recent precommitted transaction |
| precommittedTxHash | [bytes](#bytes) |  | State of the most recent precommitted transaction |






<a name="immudb.schema.InclusionProof"></a>

### InclusionProof



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| leaf | [int32](#int32) |  | Index of the leaf for which the proof is generated |
| width | [int32](#int32) |  | Width of the tree at the leaf level |
| terms | [bytes](#bytes) | repeated | Proof terms (selected hashes from the tree) |






<a name="immudb.schema.IndexNullableSettings"></a>

### IndexNullableSettings



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| flushThreshold | [NullableUint32](#immudb.schema.NullableUint32) |  | Number of new index entries between disk flushes |
| syncThreshold | [NullableUint32](#immudb.schema.NullableUint32) |  | Number of new index entries between disk flushes with file sync |
| cacheSize | [NullableUint32](#immudb.schema.NullableUint32) |  | Size of the Btree node cache in bytes |
| maxNodeSize | [NullableUint32](#immudb.schema.NullableUint32) |  | Max size of a single Btree node in bytes |
| maxActiveSnapshots | [NullableUint32](#immudb.schema.NullableUint32) |  | Maximum number of active btree snapshots |
| renewSnapRootAfter | [NullableUint64](#immudb.schema.NullableUint64) |  | Time in milliseconds between the most recent DB snapshot is automatically renewed |
| compactionThld | [NullableUint32](#immudb.schema.NullableUint32) |  | Minimum number of updates entries in the btree to allow for full compaction |
| delayDuringCompaction | [NullableUint32](#immudb.schema.NullableUint32) |  | Additional delay added during indexing when full compaction is in progress |
| nodesLogMaxOpenedFiles | [NullableUint32](#immudb.schema.NullableUint32) |  | Maximum number of simultaneously opened nodes files |
| historyLogMaxOpenedFiles | [NullableUint32](#immudb.schema.NullableUint32) |  | Maximum number of simultaneously opened node history files |
| commitLogMaxOpenedFiles | [NullableUint32](#immudb.schema.NullableUint32) |  | Maximum number of simultaneously opened commit log files |
| flushBufferSize | [NullableUint32](#immudb.schema.NullableUint32) |  | Size of the in-memory flush buffer (in bytes) |
| cleanupPercentage | [NullableFloat](#immudb.schema.NullableFloat) |  | Percentage of node files cleaned up during each flush |
| maxBulkSize | [NullableUint32](#immudb.schema.NullableUint32) |  | Maximum number of transactions indexed together |
| bulkPreparationTimeout | [NullableMilliseconds](#immudb.schema.NullableMilliseconds) |  | Maximum time waiting for more transactions to be committed and included into the same bulk |






<a name="immudb.schema.KVMetadata"></a>

### KVMetadata



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| deleted | [bool](#bool) |  | True if this entry denotes a logical deletion |
| expiration | [Expiration](#immudb.schema.Expiration) |  | Entry expiration information |
| nonIndexable | [bool](#bool) |  | If set to true, this entry will not be indexed and will only be accessed through GetAt calls |






<a name="immudb.schema.Key"></a>

### Key



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| key | [bytes](#bytes) |  |  |






<a name="immudb.schema.KeyListRequest"></a>

### KeyListRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| keys | [bytes](#bytes) | repeated | List of keys to query for |
| sinceTx | [uint64](#uint64) |  | If 0, wait for index to be up-to-date, If &gt; 0, wait for at least sinceTx transaction to be indexed |






<a name="immudb.schema.KeyPrefix"></a>

### KeyPrefix



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| prefix | [bytes](#bytes) |  |  |






<a name="immudb.schema.KeyRequest"></a>

### KeyRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| key | [bytes](#bytes) |  | Key to query for |
| atTx | [uint64](#uint64) |  | If &gt; 0, query for the value exactly at given transaction |
| sinceTx | [uint64](#uint64) |  | If 0 (and noWait=false), wait for the index to be up-to-date, If &gt; 0 (and noWait=false), wait for at lest the sinceTx transaction to be indexed |
| noWait | [bool](#bool) |  | If set to true - do not wait for any indexing update considering only the currently indexed state |
| atRevision | [int64](#int64) |  | If &gt; 0, get the nth version of the value, 1 being the first version, 2 being the second and so on If &lt; 0, get the historical nth value of the key, -1 being the previous version, -2 being the one before and so on |






<a name="immudb.schema.KeyValue"></a>

### KeyValue



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| key | [bytes](#bytes) |  |  |
| value | [bytes](#bytes) |  |  |
| metadata | [KVMetadata](#immudb.schema.KVMetadata) |  |  |






<a name="immudb.schema.LinearAdvanceProof"></a>

### LinearAdvanceProof
LinearAdvanceProof contains the proof of consistency between the consumed part of the older linear chain
and the new Merkle Tree


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| linearProofTerms | [bytes](#bytes) | repeated | terms for the linear chain |
| inclusionProofs | [InclusionProof](#immudb.schema.InclusionProof) | repeated | inclusion proofs for steps on the linear chain |






<a name="immudb.schema.LinearProof"></a>

### LinearProof
LinearProof contains the linear part of the proof (outside the main Merkle Tree)


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| sourceTxId | [uint64](#uint64) |  | Starting transaction of the proof |
| TargetTxId | [uint64](#uint64) |  | End transaction of the proof |
| terms | [bytes](#bytes) | repeated | List of terms (inner hashes of transaction entries) |






<a name="immudb.schema.LoadDatabaseRequest"></a>

### LoadDatabaseRequest
Database name


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| database | [string](#string) |  | may add createIfNotExist |






<a name="immudb.schema.LoadDatabaseResponse"></a>

### LoadDatabaseResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| database | [string](#string) |  | Database name

may add settings |






<a name="immudb.schema.LoginRequest"></a>

### LoginRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| user | [bytes](#bytes) |  | Username |
| password | [bytes](#bytes) |  | User&#39;s password |






<a name="immudb.schema.LoginResponse"></a>

### LoginResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| token | [string](#string) |  | Deprecated: use session-based authentication |
| warning | [bytes](#bytes) |  | Optional: additional warning message sent to the user (e.g. request to change the password) |






<a name="immudb.schema.MTLSConfig"></a>

### MTLSConfig
DEPRECATED


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| enabled | [bool](#bool) |  |  |






<a name="immudb.schema.NamedParam"></a>

### NamedParam



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| name | [string](#string) |  | Parameter name |
| value | [SQLValue](#immudb.schema.SQLValue) |  | Parameter value |






<a name="immudb.schema.NewTxRequest"></a>

### NewTxRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| mode | [TxMode](#immudb.schema.TxMode) |  | Transaction mode |
| snapshotMustIncludeTxID | [NullableUint64](#immudb.schema.NullableUint64) |  | An existing snapshot may be reused as long as it includes the specified transaction If not specified it will include up to the latest precommitted transaction |
| snapshotRenewalPeriod | [NullableMilliseconds](#immudb.schema.NullableMilliseconds) |  | An existing snapshot may be reused as long as it is not older than the specified timeframe |
| unsafeMVCC | [bool](#bool) |  | Indexing may not be up to date when doing MVCC |






<a name="immudb.schema.NewTxResponse"></a>

### NewTxResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| transactionID | [string](#string) |  | Internal transaction ID |






<a name="immudb.schema.NullableBool"></a>

### NullableBool



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| value | [bool](#bool) |  |  |






<a name="immudb.schema.NullableFloat"></a>

### NullableFloat



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| value | [float](#float) |  |  |






<a name="immudb.schema.NullableMilliseconds"></a>

### NullableMilliseconds



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| value | [int64](#int64) |  |  |






<a name="immudb.schema.NullableString"></a>

### NullableString



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| value | [string](#string) |  |  |






<a name="immudb.schema.NullableUint32"></a>

### NullableUint32



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| value | [uint32](#uint32) |  |  |






<a name="immudb.schema.NullableUint64"></a>

### NullableUint64



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| value | [uint64](#uint64) |  |  |






<a name="immudb.schema.Op"></a>

### Op



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| kv | [KeyValue](#immudb.schema.KeyValue) |  | Modify / add simple KV value |
| zAdd | [ZAddRequest](#immudb.schema.ZAddRequest) |  | Modify / add sorted set entry |
| ref | [ReferenceRequest](#immudb.schema.ReferenceRequest) |  | Modify / add reference |






<a name="immudb.schema.OpenSessionRequest"></a>

### OpenSessionRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| username | [bytes](#bytes) |  | Username |
| password | [bytes](#bytes) |  | Password |
| databaseName | [string](#string) |  | Database name |






<a name="immudb.schema.OpenSessionResponse"></a>

### OpenSessionResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| sessionID | [string](#string) |  | Id of the new session |
| serverUUID | [string](#string) |  | UUID of the server |






<a name="immudb.schema.Permission"></a>

### Permission



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| database | [string](#string) |  | Database name |
| permission | [uint32](#uint32) |  | Permission, 1 - read permission, 2 - read&#43;write permission, 254 - admin, 255 - sysadmin |






<a name="immudb.schema.Precondition"></a>

### Precondition



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| keyMustExist | [Precondition.KeyMustExistPrecondition](#immudb.schema.Precondition.KeyMustExistPrecondition) |  |  |
| keyMustNotExist | [Precondition.KeyMustNotExistPrecondition](#immudb.schema.Precondition.KeyMustNotExistPrecondition) |  |  |
| keyNotModifiedAfterTX | [Precondition.KeyNotModifiedAfterTXPrecondition](#immudb.schema.Precondition.KeyNotModifiedAfterTXPrecondition) |  |  |






<a name="immudb.schema.Precondition.KeyMustExistPrecondition"></a>

### Precondition.KeyMustExistPrecondition
Only succeed if given key exists


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| key | [bytes](#bytes) |  | key to check |






<a name="immudb.schema.Precondition.KeyMustNotExistPrecondition"></a>

### Precondition.KeyMustNotExistPrecondition
Only succeed if given key does not exists


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| key | [bytes](#bytes) |  | key to check |






<a name="immudb.schema.Precondition.KeyNotModifiedAfterTXPrecondition"></a>

### Precondition.KeyNotModifiedAfterTXPrecondition
Only succeed if given key was not modified after given transaction


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| key | [bytes](#bytes) |  | key to check |
| txID | [uint64](#uint64) |  | transaction id to check against |






<a name="immudb.schema.Reference"></a>

### Reference



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| tx | [uint64](#uint64) |  | Transaction if when the reference key was set |
| key | [bytes](#bytes) |  | Reference key |
| atTx | [uint64](#uint64) |  | At which transaction the key is bound, 0 if reference is not bound and should read the most recent reference |
| metadata | [KVMetadata](#immudb.schema.KVMetadata) |  | Metadata of the reference entry |
| revision | [uint64](#uint64) |  | Revision of the reference entry |






<a name="immudb.schema.ReferenceRequest"></a>

### ReferenceRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| key | [bytes](#bytes) |  | Key for the reference |
| referencedKey | [bytes](#bytes) |  | Key to be referenced |
| atTx | [uint64](#uint64) |  | If boundRef == true, id of transaction to bind with the reference |
| boundRef | [bool](#bool) |  | If true, bind the reference to particular transaction, if false, use the most recent value of the key |
| noWait | [bool](#bool) |  | If true, do not wait for the indexer to index this write operation |
| preconditions | [Precondition](#immudb.schema.Precondition) | repeated | Preconditions to be met to perform the write |






<a name="immudb.schema.ReplicaState"></a>

### ReplicaState



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| UUID | [string](#string) |  |  |
| committedTxID | [uint64](#uint64) |  |  |
| committedAlh | [bytes](#bytes) |  |  |
| precommittedTxID | [uint64](#uint64) |  |  |
| precommittedAlh | [bytes](#bytes) |  |  |






<a name="immudb.schema.ReplicationNullableSettings"></a>

### ReplicationNullableSettings



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| replica | [NullableBool](#immudb.schema.NullableBool) |  | If set to true, this database is replicating another database |
| primaryDatabase | [NullableString](#immudb.schema.NullableString) |  | Name of the database to replicate |
| primaryHost | [NullableString](#immudb.schema.NullableString) |  | Hostname of the immudb instance with database to replicate |
| primaryPort | [NullableUint32](#immudb.schema.NullableUint32) |  | Port of the immudb instance with database to replicate |
| primaryUsername | [NullableString](#immudb.schema.NullableString) |  | Username of the user with read access of the database to replicate |
| primaryPassword | [NullableString](#immudb.schema.NullableString) |  | Password of the user with read access of the database to replicate |
| syncReplication | [NullableBool](#immudb.schema.NullableBool) |  | Enable synchronous replication |
| syncAcks | [NullableUint32](#immudb.schema.NullableUint32) |  | Number of confirmations from synchronous replicas required to commit a transaction |
| prefetchTxBufferSize | [NullableUint32](#immudb.schema.NullableUint32) |  | Maximum number of prefetched transactions |
| replicationCommitConcurrency | [NullableUint32](#immudb.schema.NullableUint32) |  | Number of concurrent replications |
| allowTxDiscarding | [NullableBool](#immudb.schema.NullableBool) |  | Allow precommitted transactions to be discarded if the replica diverges from the primary |
| skipIntegrityCheck | [NullableBool](#immudb.schema.NullableBool) |  | Disable integrity check when reading data during replication |
| waitForIndexing | [NullableBool](#immudb.schema.NullableBool) |  | Wait for indexing to be up to date during replication |






<a name="immudb.schema.RetryInfo"></a>

### RetryInfo



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| retry_delay | [int32](#int32) |  | Number of milliseconds after which the request can be retried |






<a name="immudb.schema.Row"></a>

### Row



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| columns | [string](#string) | repeated | Column names |
| values | [SQLValue](#immudb.schema.SQLValue) | repeated | Column values |






<a name="immudb.schema.SQLEntry"></a>

### SQLEntry



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| tx | [uint64](#uint64) |  | Id of the transaction when the row was added / modified |
| key | [bytes](#bytes) |  | Raw key of the row |
| value | [bytes](#bytes) |  | Raw value of the row |
| metadata | [KVMetadata](#immudb.schema.KVMetadata) |  | Metadata of the raw value |






<a name="immudb.schema.SQLExecRequest"></a>

### SQLExecRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| sql | [string](#string) |  | SQL query |
| params | [NamedParam](#immudb.schema.NamedParam) | repeated | Named query parameters |
| noWait | [bool](#bool) |  | If true, do not wait for the indexer to index written changes |






<a name="immudb.schema.SQLExecResult"></a>

### SQLExecResult



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| txs | [CommittedSQLTx](#immudb.schema.CommittedSQLTx) | repeated | List of committed transactions as a result of the exec operation |
| ongoingTx | [bool](#bool) |  | If true, there&#39;s an ongoing transaction after exec completes |






<a name="immudb.schema.SQLGetRequest"></a>

### SQLGetRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| table | [string](#string) |  | Table name |
| pkValues | [SQLValue](#immudb.schema.SQLValue) | repeated | Values of the primary key |
| atTx | [uint64](#uint64) |  | Id of the transaction at which the row was added / modified |
| sinceTx | [uint64](#uint64) |  | If &gt; 0, do not wait for the indexer to index all entries, only require entries up to sinceTx to be indexed |






<a name="immudb.schema.SQLQueryRequest"></a>

### SQLQueryRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| sql | [string](#string) |  | SQL query |
| params | [NamedParam](#immudb.schema.NamedParam) | repeated | Named query parameters |
| reuseSnapshot | [bool](#bool) |  | **Deprecated.** If true, reuse previously opened snapshot |
| acceptStream | [bool](#bool) |  | Wheter the client accepts a streaming response |






<a name="immudb.schema.SQLQueryResult"></a>

### SQLQueryResult



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| columns | [Column](#immudb.schema.Column) | repeated | Result columns description |
| rows | [Row](#immudb.schema.Row) | repeated | Result rows |






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
| f | [double](#double) |  |  |






<a name="immudb.schema.ScanRequest"></a>

### ScanRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| seekKey | [bytes](#bytes) |  | If not empty, continue scan at (when inclusiveSeek == true) or after (when inclusiveSeek == false) that key |
| endKey | [bytes](#bytes) |  | stop at (when inclusiveEnd == true) or before (when inclusiveEnd == false) that key |
| prefix | [bytes](#bytes) |  | search for entries with this prefix only |
| desc | [bool](#bool) |  | If set to true, sort items in descending order |
| limit | [uint64](#uint64) |  | maximum number of entries to get, if not specified, the default value is used |
| sinceTx | [uint64](#uint64) |  | If non-zero, only require transactions up to this transaction to be indexed, newer transaction may still be pending |
| noWait | [bool](#bool) |  | Deprecated: If set to true, do not wait for indexing to be done before finishing this call |
| inclusiveSeek | [bool](#bool) |  | If set to true, results will include seekKey |
| inclusiveEnd | [bool](#bool) |  | If set to true, results will include endKey if needed |
| offset | [uint64](#uint64) |  | Specify the initial entry to be returned by excluding the initial set of entries |






<a name="immudb.schema.Score"></a>

### Score



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| score | [double](#double) |  | Entry&#39;s score value |






<a name="immudb.schema.ServerInfoRequest"></a>

### ServerInfoRequest
ServerInfoRequest exists to provide extensibility for rpc ServerInfo.






<a name="immudb.schema.ServerInfoResponse"></a>

### ServerInfoResponse
ServerInfoResponse contains information about the server instance.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| version | [string](#string) |  | The version of the server instance. |
| startedAt | [int64](#int64) |  | Unix timestamp (seconds) indicating when the server process has been started. |
| numTransactions | [int64](#int64) |  | Total number of transactions across all databases. |
| numDatabases | [int32](#int32) |  | Total number of databases present. |
| databasesDiskSize | [int64](#int64) |  | Total disk size used by all databases. |






<a name="immudb.schema.SetActiveUserRequest"></a>

### SetActiveUserRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| active | [bool](#bool) |  | If true, the user is active |
| username | [string](#string) |  | Name of the user to activate / deactivate |






<a name="immudb.schema.SetRequest"></a>

### SetRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| KVs | [KeyValue](#immudb.schema.KeyValue) | repeated | List of KV entries to set |
| noWait | [bool](#bool) |  | If set to true, do not wait for indexer to index ne entries |
| preconditions | [Precondition](#immudb.schema.Precondition) | repeated | Preconditions to be met to perform the write |






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
| tableName | [string](#string) |  | Table name |






<a name="immudb.schema.TruncateDatabaseRequest"></a>

### TruncateDatabaseRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| database | [string](#string) |  | Database name |
| retentionPeriod | [int64](#int64) |  | Retention Period of data |






<a name="immudb.schema.TruncateDatabaseResponse"></a>

### TruncateDatabaseResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| database | [string](#string) |  | Database name |






<a name="immudb.schema.TruncationNullableSettings"></a>

### TruncationNullableSettings



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| retentionPeriod | [NullableMilliseconds](#immudb.schema.NullableMilliseconds) |  | Retention Period for data in the database |
| truncationFrequency | [NullableMilliseconds](#immudb.schema.NullableMilliseconds) |  | Truncation Frequency for the database |






<a name="immudb.schema.Tx"></a>

### Tx



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| header | [TxHeader](#immudb.schema.TxHeader) |  | Transaction header |
| entries | [TxEntry](#immudb.schema.TxEntry) | repeated | Raw entry values |
| kvEntries | [Entry](#immudb.schema.Entry) | repeated | KV entries in the transaction (parsed) |
| zEntries | [ZEntry](#immudb.schema.ZEntry) | repeated | Sorted Set entries in the transaction (parsed) |






<a name="immudb.schema.TxEntry"></a>

### TxEntry



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| key | [bytes](#bytes) |  | Raw key value (contains 1-byte prefix for kind of the key) |
| hValue | [bytes](#bytes) |  | Value hash |
| vLen | [int32](#int32) |  | Value length |
| metadata | [KVMetadata](#immudb.schema.KVMetadata) |  | Entry metadata |
| value | [bytes](#bytes) |  | value, must be ignored when len(value) == 0 and vLen &gt; 0. Otherwise sha256(value) must be equal to hValue. |






<a name="immudb.schema.TxHeader"></a>

### TxHeader



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| id | [uint64](#uint64) |  | Transaction ID |
| prevAlh | [bytes](#bytes) |  | State value (Accumulative Hash - Alh) of the previous transaction |
| ts | [int64](#int64) |  | Unix timestamp of the transaction (in seconds) |
| nentries | [int32](#int32) |  | Number of entries in a transaction |
| eH | [bytes](#bytes) |  | Entries Hash - cumulative hash of all entries in the transaction |
| blTxId | [uint64](#uint64) |  | Binary linking tree transaction ID (ID of last transaction already in the main Merkle Tree) |
| blRoot | [bytes](#bytes) |  | Binary linking tree root (Root hash of the Merkle Tree) |
| version | [int32](#int32) |  | Header version |
| metadata | [TxMetadata](#immudb.schema.TxMetadata) |  | Transaction metadata |






<a name="immudb.schema.TxList"></a>

### TxList



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| txs | [Tx](#immudb.schema.Tx) | repeated | List of transactions |






<a name="immudb.schema.TxMetadata"></a>

### TxMetadata
TxMetadata contains metadata set to whole transaction


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| truncatedTxID | [uint64](#uint64) |  | Entry expiration information |
| extra | [bytes](#bytes) |  | Extra data |






<a name="immudb.schema.TxRequest"></a>

### TxRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| tx | [uint64](#uint64) |  | Transaction id to query for |
| entriesSpec | [EntriesSpec](#immudb.schema.EntriesSpec) |  | Specification for parsing entries, if empty, entries are returned in raw form |
| sinceTx | [uint64](#uint64) |  | If &gt; 0, do not wait for the indexer to index all entries, only require entries up to sinceTx to be indexed, will affect resolving references |
| noWait | [bool](#bool) |  | Deprecated: If set to true, do not wait for the indexer to be up to date |
| keepReferencesUnresolved | [bool](#bool) |  | If set to true, do not resolve references (avoid looking up final values if not needed) |






<a name="immudb.schema.TxScanRequest"></a>

### TxScanRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| initialTx | [uint64](#uint64) |  | ID of the transaction where scanning should start |
| limit | [uint32](#uint32) |  | Maximum number of transactions to scan, when not specified the default limit is used |
| desc | [bool](#bool) |  | If set to true, scan transactions in descending order |
| entriesSpec | [EntriesSpec](#immudb.schema.EntriesSpec) |  | Specification of how to parse entries |
| sinceTx | [uint64](#uint64) |  | If &gt; 0, do not wait for the indexer to index all entries, only require entries up to sinceTx to be indexed, will affect resolving references |
| noWait | [bool](#bool) |  | Deprecated: If set to true, do not wait for the indexer to be up to date |






<a name="immudb.schema.UnloadDatabaseRequest"></a>

### UnloadDatabaseRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| database | [string](#string) |  | Database name |






<a name="immudb.schema.UnloadDatabaseResponse"></a>

### UnloadDatabaseResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| database | [string](#string) |  | Database name |






<a name="immudb.schema.UpdateDatabaseRequest"></a>

### UpdateDatabaseRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| database | [string](#string) |  | Database name |
| settings | [DatabaseNullableSettings](#immudb.schema.DatabaseNullableSettings) |  | Updated settings |






<a name="immudb.schema.UpdateDatabaseResponse"></a>

### UpdateDatabaseResponse
Reserved to reply with more advanced response later


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| database | [string](#string) |  | Database name |
| settings | [DatabaseNullableSettings](#immudb.schema.DatabaseNullableSettings) |  | Current database settings |






<a name="immudb.schema.UseDatabaseReply"></a>

### UseDatabaseReply



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| token | [string](#string) |  | Deprecated: database access token |






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
| user | [bytes](#bytes) |  | Username |
| permissions | [Permission](#immudb.schema.Permission) | repeated | List of permissions for the user |
| createdby | [string](#string) |  | Name of the creator user |
| createdat | [string](#string) |  | Time when the user was created |
| active | [bool](#bool) |  | Flag indicating whether the user is active or not |
| sqlPrivileges | [SQLPrivilege](#immudb.schema.SQLPrivilege) | repeated | List of SQL privileges |






<a name="immudb.schema.UserList"></a>

### UserList



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| users | [User](#immudb.schema.User) | repeated | List of users |






<a name="immudb.schema.UserRequest"></a>

### UserRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| user | [bytes](#bytes) |  | Username |






<a name="immudb.schema.VerifiableEntry"></a>

### VerifiableEntry



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| entry | [Entry](#immudb.schema.Entry) |  | Entry to verify |
| verifiableTx | [VerifiableTx](#immudb.schema.VerifiableTx) |  | Transaction to verify |
| inclusionProof | [InclusionProof](#immudb.schema.InclusionProof) |  | Proof for inclusion of the entry within the transaction |






<a name="immudb.schema.VerifiableGetRequest"></a>

### VerifiableGetRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| keyRequest | [KeyRequest](#immudb.schema.KeyRequest) |  | Key to read |
| proveSinceTx | [uint64](#uint64) |  | When generating the proof, generate consistency proof with state from this transaction |






<a name="immudb.schema.VerifiableReferenceRequest"></a>

### VerifiableReferenceRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| referenceRequest | [ReferenceRequest](#immudb.schema.ReferenceRequest) |  | Reference data |
| proveSinceTx | [uint64](#uint64) |  | When generating the proof, generate consistency proof with state from this transaction |






<a name="immudb.schema.VerifiableSQLEntry"></a>

### VerifiableSQLEntry



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| sqlEntry | [SQLEntry](#immudb.schema.SQLEntry) |  | Raw row entry data |
| verifiableTx | [VerifiableTx](#immudb.schema.VerifiableTx) |  | Verifiable transaction of the row |
| inclusionProof | [InclusionProof](#immudb.schema.InclusionProof) |  | Inclusion proof of the row in the transaction |
| DatabaseId | [uint32](#uint32) |  | Internal ID of the database (used to validate raw entry values) |
| TableId | [uint32](#uint32) |  | Internal ID of the table (used to validate raw entry values) |
| PKIDs | [uint32](#uint32) | repeated | Internal IDs of columns for the primary key (used to validate raw entry values) |
| ColNamesById | [VerifiableSQLEntry.ColNamesByIdEntry](#immudb.schema.VerifiableSQLEntry.ColNamesByIdEntry) | repeated | Mapping of used column IDs to their names |
| ColIdsByName | [VerifiableSQLEntry.ColIdsByNameEntry](#immudb.schema.VerifiableSQLEntry.ColIdsByNameEntry) | repeated | Mapping of column names to their IDS |
| ColTypesById | [VerifiableSQLEntry.ColTypesByIdEntry](#immudb.schema.VerifiableSQLEntry.ColTypesByIdEntry) | repeated | Mapping of column IDs to their types |
| ColLenById | [VerifiableSQLEntry.ColLenByIdEntry](#immudb.schema.VerifiableSQLEntry.ColLenByIdEntry) | repeated | Mapping of column IDs to their length constraints |
| MaxColId | [uint32](#uint32) |  | Variable is used to assign unique ids to new columns as they are created |






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
| sqlGetRequest | [SQLGetRequest](#immudb.schema.SQLGetRequest) |  | Data of row to query |
| proveSinceTx | [uint64](#uint64) |  | When generating the proof, generate consistency proof with state from this transaction |






<a name="immudb.schema.VerifiableSetRequest"></a>

### VerifiableSetRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| setRequest | [SetRequest](#immudb.schema.SetRequest) |  | Keys to set |
| proveSinceTx | [uint64](#uint64) |  | When generating the proof, generate consistency proof with state from this transaction |






<a name="immudb.schema.VerifiableTx"></a>

### VerifiableTx



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| tx | [Tx](#immudb.schema.Tx) |  | Transaction to verify |
| dualProof | [DualProof](#immudb.schema.DualProof) |  | Proof for the transaction |
| signature | [Signature](#immudb.schema.Signature) |  | Signature for the new state value |






<a name="immudb.schema.VerifiableTxRequest"></a>

### VerifiableTxRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| tx | [uint64](#uint64) |  | Transaction ID |
| proveSinceTx | [uint64](#uint64) |  | When generating the proof, generate consistency proof with state from this transaction |
| entriesSpec | [EntriesSpec](#immudb.schema.EntriesSpec) |  | Specification of how to parse entries |
| sinceTx | [uint64](#uint64) |  | If &gt; 0, do not wait for the indexer to index all entries, only require entries up to sinceTx to be indexed, will affect resolving references |
| noWait | [bool](#bool) |  | Deprecated: If set to true, do not wait for the indexer to be up to date |
| keepReferencesUnresolved | [bool](#bool) |  | If set to true, do not resolve references (avoid looking up final values if not needed) |






<a name="immudb.schema.VerifiableTxV2"></a>

### VerifiableTxV2



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| tx | [Tx](#immudb.schema.Tx) |  | Transaction to verify |
| dualProof | [DualProofV2](#immudb.schema.DualProofV2) |  | Proof for the transaction |
| signature | [Signature](#immudb.schema.Signature) |  | Signature for the new state value |






<a name="immudb.schema.VerifiableZAddRequest"></a>

### VerifiableZAddRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| zAddRequest | [ZAddRequest](#immudb.schema.ZAddRequest) |  | Data for new sorted set entry |
| proveSinceTx | [uint64](#uint64) |  | When generating the proof, generate consistency proof with state from this transaction |






<a name="immudb.schema.ZAddRequest"></a>

### ZAddRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| set | [bytes](#bytes) |  | Name of the sorted set |
| score | [double](#double) |  | Score of the new entry |
| key | [bytes](#bytes) |  | Referenced key |
| atTx | [uint64](#uint64) |  | If boundRef == true, id of the transaction to bind with the reference |
| boundRef | [bool](#bool) |  | If true, bind the reference to particular transaction, if false, use the most recent value of the key |
| noWait | [bool](#bool) |  | If true, do not wait for the indexer to index this write operation |






<a name="immudb.schema.ZEntries"></a>

### ZEntries



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| entries | [ZEntry](#immudb.schema.ZEntry) | repeated |  |






<a name="immudb.schema.ZEntry"></a>

### ZEntry



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| set | [bytes](#bytes) |  | Name of the sorted set |
| key | [bytes](#bytes) |  | Referenced key |
| entry | [Entry](#immudb.schema.Entry) |  | Referenced entry |
| score | [double](#double) |  | Sorted set element&#39;s score |
| atTx | [uint64](#uint64) |  | At which transaction the key is bound, 0 if reference is not bound and should read the most recent reference |






<a name="immudb.schema.ZScanRequest"></a>

### ZScanRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| set | [bytes](#bytes) |  | Name of the sorted set |
| seekKey | [bytes](#bytes) |  | Key to continue the search at |
| seekScore | [double](#double) |  | Score of the entry to continue the search at |
| seekAtTx | [uint64](#uint64) |  | AtTx of the entry to continue the search at |
| inclusiveSeek | [bool](#bool) |  | If true, include the entry given with the `seekXXX` attributes, if false, skip the entry and start after that one |
| limit | [uint64](#uint64) |  | Maximum number of entries to return, if 0, the default limit will be used |
| desc | [bool](#bool) |  | If true, scan entries in descending order |
| minScore | [Score](#immudb.schema.Score) |  | Minimum score of entries to scan |
| maxScore | [Score](#immudb.schema.Score) |  | Maximum score of entries to scan |
| sinceTx | [uint64](#uint64) |  | If &gt; 0, do not wait for the indexer to index all entries, only require entries up to sinceTx to be indexed |
| noWait | [bool](#bool) |  | Deprecated: If set to true, do not wait for the indexer to be up to date |
| offset | [uint64](#uint64) |  | Specify the index of initial entry to be returned by excluding the initial set of entries (alternative to seekXXX attributes) |





 


<a name="immudb.schema.EntryTypeAction"></a>

### EntryTypeAction


| Name | Number | Description |
| ---- | ------ | ----------- |
| EXCLUDE | 0 | Exclude entries from the result |
| ONLY_DIGEST | 1 | Provide keys in raw (unparsed) form and only the digest of the value |
| RAW_VALUE | 2 | Provide keys and values in raw form |
| RESOLVE | 3 | Provide parsed keys and values and resolve values if needed |



<a name="immudb.schema.PermissionAction"></a>

### PermissionAction


| Name | Number | Description |
| ---- | ------ | ----------- |
| GRANT | 0 | Grant permission |
| REVOKE | 1 | Revoke permission |



<a name="immudb.schema.SQLPrivilege"></a>

### SQLPrivilege


| Name | Number | Description |
| ---- | ------ | ----------- |
| UNKNOWN | 0 |  |
| SELECT | 1 |  |
| CREATE | 2 |  |
| INSERT | 3 |  |
| UPDATE | 4 |  |
| DELETE | 5 |  |
| DROP | 6 |  |
| ALTER | 7 |  |



<a name="immudb.schema.TxMode"></a>

### TxMode


| Name | Number | Description |
| ---- | ------ | ----------- |
| ReadOnly | 0 | Read-only transaction |
| WriteOnly | 1 | Write-only transaction |
| ReadWrite | 2 | Read-write transaction |


 

 


<a name="immudb.schema.ImmuService"></a>

### ImmuService
immudb gRPC &amp; REST service

| Method Name | Request Type | Response Type | Description |
| ----------- | ------------ | ------------- | ------------|
| ListUsers | [.google.protobuf.Empty](#google.protobuf.Empty) | [UserList](#immudb.schema.UserList) |  |
| CreateUser | [CreateUserRequest](#immudb.schema.CreateUserRequest) | [.google.protobuf.Empty](#google.protobuf.Empty) |  |
| ChangePassword | [ChangePasswordRequest](#immudb.schema.ChangePasswordRequest) | [.google.protobuf.Empty](#google.protobuf.Empty) |  |
| ChangePermission | [ChangePermissionRequest](#immudb.schema.ChangePermissionRequest) | [.google.protobuf.Empty](#google.protobuf.Empty) |  |
| ChangeSQLPrivileges | [ChangeSQLPrivilegesRequest](#immudb.schema.ChangeSQLPrivilegesRequest) | [ChangeSQLPrivilegesResponse](#immudb.schema.ChangeSQLPrivilegesResponse) |  |
| SetActiveUser | [SetActiveUserRequest](#immudb.schema.SetActiveUserRequest) | [.google.protobuf.Empty](#google.protobuf.Empty) |  |
| UpdateAuthConfig | [AuthConfig](#immudb.schema.AuthConfig) | [.google.protobuf.Empty](#google.protobuf.Empty) |  |
| UpdateMTLSConfig | [MTLSConfig](#immudb.schema.MTLSConfig) | [.google.protobuf.Empty](#google.protobuf.Empty) |  |
| OpenSession | [OpenSessionRequest](#immudb.schema.OpenSessionRequest) | [OpenSessionResponse](#immudb.schema.OpenSessionResponse) |  |
| CloseSession | [.google.protobuf.Empty](#google.protobuf.Empty) | [.google.protobuf.Empty](#google.protobuf.Empty) |  |
| KeepAlive | [.google.protobuf.Empty](#google.protobuf.Empty) | [.google.protobuf.Empty](#google.protobuf.Empty) |  |
| NewTx | [NewTxRequest](#immudb.schema.NewTxRequest) | [NewTxResponse](#immudb.schema.NewTxResponse) |  |
| Commit | [.google.protobuf.Empty](#google.protobuf.Empty) | [CommittedSQLTx](#immudb.schema.CommittedSQLTx) |  |
| Rollback | [.google.protobuf.Empty](#google.protobuf.Empty) | [.google.protobuf.Empty](#google.protobuf.Empty) |  |
| TxSQLExec | [SQLExecRequest](#immudb.schema.SQLExecRequest) | [.google.protobuf.Empty](#google.protobuf.Empty) |  |
| TxSQLQuery | [SQLQueryRequest](#immudb.schema.SQLQueryRequest) | [SQLQueryResult](#immudb.schema.SQLQueryResult) stream |  |
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
| ServerInfo | [ServerInfoRequest](#immudb.schema.ServerInfoRequest) | [ServerInfoResponse](#immudb.schema.ServerInfoResponse) | ServerInfo returns information about the server instance. ServerInfoRequest is defined for future extensions. |
| Health | [.google.protobuf.Empty](#google.protobuf.Empty) | [HealthResponse](#immudb.schema.HealthResponse) | DEPRECATED: Use ServerInfo |
| DatabaseHealth | [.google.protobuf.Empty](#google.protobuf.Empty) | [DatabaseHealthResponse](#immudb.schema.DatabaseHealthResponse) |  |
| CurrentState | [.google.protobuf.Empty](#google.protobuf.Empty) | [ImmutableState](#immudb.schema.ImmutableState) |  |
| SetReference | [ReferenceRequest](#immudb.schema.ReferenceRequest) | [TxHeader](#immudb.schema.TxHeader) |  |
| VerifiableSetReference | [VerifiableReferenceRequest](#immudb.schema.VerifiableReferenceRequest) | [VerifiableTx](#immudb.schema.VerifiableTx) |  |
| ZAdd | [ZAddRequest](#immudb.schema.ZAddRequest) | [TxHeader](#immudb.schema.TxHeader) |  |
| VerifiableZAdd | [VerifiableZAddRequest](#immudb.schema.VerifiableZAddRequest) | [VerifiableTx](#immudb.schema.VerifiableTx) |  |
| ZScan | [ZScanRequest](#immudb.schema.ZScanRequest) | [ZEntries](#immudb.schema.ZEntries) |  |
| CreateDatabase | [Database](#immudb.schema.Database) | [.google.protobuf.Empty](#google.protobuf.Empty) | DEPRECATED: Use CreateDatabaseV2 |
| CreateDatabaseWith | [DatabaseSettings](#immudb.schema.DatabaseSettings) | [.google.protobuf.Empty](#google.protobuf.Empty) | DEPRECATED: Use CreateDatabaseV2 |
| CreateDatabaseV2 | [CreateDatabaseRequest](#immudb.schema.CreateDatabaseRequest) | [CreateDatabaseResponse](#immudb.schema.CreateDatabaseResponse) |  |
| LoadDatabase | [LoadDatabaseRequest](#immudb.schema.LoadDatabaseRequest) | [LoadDatabaseResponse](#immudb.schema.LoadDatabaseResponse) |  |
| UnloadDatabase | [UnloadDatabaseRequest](#immudb.schema.UnloadDatabaseRequest) | [UnloadDatabaseResponse](#immudb.schema.UnloadDatabaseResponse) |  |
| DeleteDatabase | [DeleteDatabaseRequest](#immudb.schema.DeleteDatabaseRequest) | [DeleteDatabaseResponse](#immudb.schema.DeleteDatabaseResponse) |  |
| DatabaseList | [.google.protobuf.Empty](#google.protobuf.Empty) | [DatabaseListResponse](#immudb.schema.DatabaseListResponse) | DEPRECATED: Use DatabaseListV2 |
| DatabaseListV2 | [DatabaseListRequestV2](#immudb.schema.DatabaseListRequestV2) | [DatabaseListResponseV2](#immudb.schema.DatabaseListResponseV2) |  |
| UseDatabase | [Database](#immudb.schema.Database) | [UseDatabaseReply](#immudb.schema.UseDatabaseReply) |  |
| UpdateDatabase | [DatabaseSettings](#immudb.schema.DatabaseSettings) | [.google.protobuf.Empty](#google.protobuf.Empty) | DEPRECATED: Use UpdateDatabaseV2 |
| UpdateDatabaseV2 | [UpdateDatabaseRequest](#immudb.schema.UpdateDatabaseRequest) | [UpdateDatabaseResponse](#immudb.schema.UpdateDatabaseResponse) |  |
| GetDatabaseSettings | [.google.protobuf.Empty](#google.protobuf.Empty) | [DatabaseSettings](#immudb.schema.DatabaseSettings) | DEPRECATED: Use GetDatabaseSettingsV2 |
| GetDatabaseSettingsV2 | [DatabaseSettingsRequest](#immudb.schema.DatabaseSettingsRequest) | [DatabaseSettingsResponse](#immudb.schema.DatabaseSettingsResponse) |  |
| FlushIndex | [FlushIndexRequest](#immudb.schema.FlushIndexRequest) | [FlushIndexResponse](#immudb.schema.FlushIndexResponse) |  |
| CompactIndex | [.google.protobuf.Empty](#google.protobuf.Empty) | [.google.protobuf.Empty](#google.protobuf.Empty) |  |
| streamGet | [KeyRequest](#immudb.schema.KeyRequest) | [Chunk](#immudb.schema.Chunk) stream | Streams |
| streamSet | [Chunk](#immudb.schema.Chunk) stream | [TxHeader](#immudb.schema.TxHeader) |  |
| streamVerifiableGet | [VerifiableGetRequest](#immudb.schema.VerifiableGetRequest) | [Chunk](#immudb.schema.Chunk) stream |  |
| streamVerifiableSet | [Chunk](#immudb.schema.Chunk) stream | [VerifiableTx](#immudb.schema.VerifiableTx) |  |
| streamScan | [ScanRequest](#immudb.schema.ScanRequest) | [Chunk](#immudb.schema.Chunk) stream |  |
| streamZScan | [ZScanRequest](#immudb.schema.ZScanRequest) | [Chunk](#immudb.schema.Chunk) stream |  |
| streamHistory | [HistoryRequest](#immudb.schema.HistoryRequest) | [Chunk](#immudb.schema.Chunk) stream |  |
| streamExecAll | [Chunk](#immudb.schema.Chunk) stream | [TxHeader](#immudb.schema.TxHeader) |  |
| exportTx | [ExportTxRequest](#immudb.schema.ExportTxRequest) | [Chunk](#immudb.schema.Chunk) stream | Replication |
| replicateTx | [Chunk](#immudb.schema.Chunk) stream | [TxHeader](#immudb.schema.TxHeader) |  |
| streamExportTx | [ExportTxRequest](#immudb.schema.ExportTxRequest) stream | [Chunk](#immudb.schema.Chunk) stream |  |
| SQLExec | [SQLExecRequest](#immudb.schema.SQLExecRequest) | [SQLExecResult](#immudb.schema.SQLExecResult) |  |
| UnarySQLQuery | [SQLQueryRequest](#immudb.schema.SQLQueryRequest) | [SQLQueryResult](#immudb.schema.SQLQueryResult) | For backward compatibility with the grpc-gateway API |
| SQLQuery | [SQLQueryRequest](#immudb.schema.SQLQueryRequest) | [SQLQueryResult](#immudb.schema.SQLQueryResult) stream |  |
| ListTables | [.google.protobuf.Empty](#google.protobuf.Empty) | [SQLQueryResult](#immudb.schema.SQLQueryResult) |  |
| DescribeTable | [Table](#immudb.schema.Table) | [SQLQueryResult](#immudb.schema.SQLQueryResult) |  |
| VerifiableSQLGet | [VerifiableSQLGetRequest](#immudb.schema.VerifiableSQLGetRequest) | [VerifiableSQLEntry](#immudb.schema.VerifiableSQLEntry) |  |
| TruncateDatabase | [TruncateDatabaseRequest](#immudb.schema.TruncateDatabaseRequest) | [TruncateDatabaseResponse](#immudb.schema.TruncateDatabaseResponse) |  |

 



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


# Protocol Documentation
<a name="top"></a>

## Table of Contents

- [schema.proto](#schema.proto)
    - [AuthConfig](#immudb.schema.AuthConfig)
    - [ChangePasswordRequest](#immudb.schema.ChangePasswordRequest)
    - [ChangePermissionRequest](#immudb.schema.ChangePermissionRequest)
    - [CreateUserRequest](#immudb.schema.CreateUserRequest)
    - [Database](#immudb.schema.Database)
    - [DatabaseListResponse](#immudb.schema.DatabaseListResponse)
    - [DualProof](#immudb.schema.DualProof)
    - [HealthResponse](#immudb.schema.HealthResponse)
    - [HistoryRequest](#immudb.schema.HistoryRequest)
    - [IScanRequest](#immudb.schema.IScanRequest)
    - [ImmutableState](#immudb.schema.ImmutableState)
    - [InclusionProof](#immudb.schema.InclusionProof)
    - [Item](#immudb.schema.Item)
    - [ItemList](#immudb.schema.ItemList)
    - [ItemsCount](#immudb.schema.ItemsCount)
    - [Key](#immudb.schema.Key)
    - [KeyListRequest](#immudb.schema.KeyListRequest)
    - [KeyPrefix](#immudb.schema.KeyPrefix)
    - [KeyRequest](#immudb.schema.KeyRequest)
    - [KeyValue](#immudb.schema.KeyValue)
    - [Layer](#immudb.schema.Layer)
    - [LinearProof](#immudb.schema.LinearProof)
    - [LoginRequest](#immudb.schema.LoginRequest)
    - [LoginResponse](#immudb.schema.LoginResponse)
    - [MTLSConfig](#immudb.schema.MTLSConfig)
    - [Node](#immudb.schema.Node)
    - [Op](#immudb.schema.Op)
    - [Ops](#immudb.schema.Ops)
    - [Page](#immudb.schema.Page)
    - [Permission](#immudb.schema.Permission)
    - [Reference](#immudb.schema.Reference)
    - [ScanRequest](#immudb.schema.ScanRequest)
    - [Score](#immudb.schema.Score)
    - [SetActiveUserRequest](#immudb.schema.SetActiveUserRequest)
    - [SetRequest](#immudb.schema.SetRequest)
    - [Signature](#immudb.schema.Signature)
    - [Tree](#immudb.schema.Tree)
    - [Tx](#immudb.schema.Tx)
    - [TxEntry](#immudb.schema.TxEntry)
    - [TxMetadata](#immudb.schema.TxMetadata)
    - [TxRequest](#immudb.schema.TxRequest)
    - [UseDatabaseReply](#immudb.schema.UseDatabaseReply)
    - [User](#immudb.schema.User)
    - [UserList](#immudb.schema.UserList)
    - [UserRequest](#immudb.schema.UserRequest)
    - [VerifiableGetRequest](#immudb.schema.VerifiableGetRequest)
    - [VerifiableItem](#immudb.schema.VerifiableItem)
    - [VerifiableReferenceRequest](#immudb.schema.VerifiableReferenceRequest)
    - [VerifiableSetRequest](#immudb.schema.VerifiableSetRequest)
    - [VerifiableTx](#immudb.schema.VerifiableTx)
    - [VerifiableTxRequest](#immudb.schema.VerifiableTxRequest)
    - [VerifiableZAddRequest](#immudb.schema.VerifiableZAddRequest)
    - [ZAddRequest](#immudb.schema.ZAddRequest)
    - [ZItem](#immudb.schema.ZItem)
    - [ZItemList](#immudb.schema.ZItemList)
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
| databasename | [string](#string) |  |  |






<a name="immudb.schema.DatabaseListResponse"></a>

### DatabaseListResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| databases | [Database](#immudb.schema.Database) | repeated |  |






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
| limit | [uint64](#uint64) |  |  |
| reverse | [bool](#bool) |  |  |
| FromTx | [uint64](#uint64) |  |  |






<a name="immudb.schema.IScanRequest"></a>

### IScanRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| pageSize | [uint64](#uint64) |  |  |
| pageNumber | [uint64](#uint64) |  |  |






<a name="immudb.schema.ImmutableState"></a>

### ImmutableState



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
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






<a name="immudb.schema.Item"></a>

### Item



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| tx | [uint64](#uint64) |  |  |
| key | [bytes](#bytes) |  |  |
| value | [bytes](#bytes) |  |  |






<a name="immudb.schema.ItemList"></a>

### ItemList



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| items | [Item](#immudb.schema.Item) | repeated |  |






<a name="immudb.schema.ItemsCount"></a>

### ItemsCount



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| count | [uint64](#uint64) |  |  |






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
| sinceTx | [uint64](#uint64) |  |  |






<a name="immudb.schema.KeyValue"></a>

### KeyValue



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| key | [bytes](#bytes) |  |  |
| value | [bytes](#bytes) |  |  |






<a name="immudb.schema.Layer"></a>

### Layer



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| l | [Node](#immudb.schema.Node) | repeated |  |






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






<a name="immudb.schema.Node"></a>

### Node



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| i | [bytes](#bytes) |  |  |
| h | [bytes](#bytes) |  |  |
| refk | [bytes](#bytes) |  |  |
| ref | [bool](#bool) |  |  |
| cache | [bool](#bool) |  |  |
| root | [bool](#bool) |  |  |






<a name="immudb.schema.Op"></a>

### Op



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| kv | [KeyValue](#immudb.schema.KeyValue) |  |  |
| zAdd | [ZAddRequest](#immudb.schema.ZAddRequest) |  |  |
| ref | [Reference](#immudb.schema.Reference) |  |  |






<a name="immudb.schema.Ops"></a>

### Ops



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| Operations | [Op](#immudb.schema.Op) | repeated |  |






<a name="immudb.schema.Page"></a>

### Page



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| items | [Item](#immudb.schema.Item) | repeated |  |
| pageNum | [uint64](#uint64) |  |  |
| more | [bool](#bool) |  |  |






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
| reference | [bytes](#bytes) |  |  |
| key | [bytes](#bytes) |  |  |
| atTx | [uint64](#uint64) |  |  |






<a name="immudb.schema.ScanRequest"></a>

### ScanRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| prefix | [bytes](#bytes) |  |  |
| offset | [bytes](#bytes) |  |  |
| limit | [uint64](#uint64) |  |  |
| reverse | [bool](#bool) |  |  |
| deep | [bool](#bool) |  | TODO: ? |






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






<a name="immudb.schema.Signature"></a>

### Signature



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| publicKey | [bytes](#bytes) |  |  |
| signature | [bytes](#bytes) |  |  |






<a name="immudb.schema.Tree"></a>

### Tree



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| t | [Layer](#immudb.schema.Layer) | repeated |  |






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
| vOff | [int64](#int64) |  |  |
| vLen | [int32](#int32) |  |  |






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






<a name="immudb.schema.UseDatabaseReply"></a>

### UseDatabaseReply



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| token | [string](#string) |  |  |






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






<a name="immudb.schema.VerifiableGetRequest"></a>

### VerifiableGetRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| keyRequest | [KeyRequest](#immudb.schema.KeyRequest) |  |  |
| proveSinceTx | [uint64](#uint64) |  |  |






<a name="immudb.schema.VerifiableItem"></a>

### VerifiableItem



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| item | [Item](#immudb.schema.Item) |  |  |
| verifiableTx | [VerifiableTx](#immudb.schema.VerifiableTx) |  |  |
| inclusionProof | [InclusionProof](#immudb.schema.InclusionProof) |  |  |






<a name="immudb.schema.VerifiableReferenceRequest"></a>

### VerifiableReferenceRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| reference | [Reference](#immudb.schema.Reference) |  |  |
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
| score | [Score](#immudb.schema.Score) |  |  |
| key | [bytes](#bytes) |  |  |
| atTx | [uint64](#uint64) |  |  |






<a name="immudb.schema.ZItem"></a>

### ZItem



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| item | [Item](#immudb.schema.Item) |  |  |
| score | [double](#double) |  |  |
| currentOffset | [bytes](#bytes) |  | TODO: ? |
| tx | [uint64](#uint64) |  |  |






<a name="immudb.schema.ZItemList"></a>

### ZItemList



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| items | [ZItem](#immudb.schema.ZItem) | repeated |  |






<a name="immudb.schema.ZScanRequest"></a>

### ZScanRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| set | [bytes](#bytes) |  |  |
| offset | [bytes](#bytes) |  |  |
| limit | [uint64](#uint64) |  |  |
| reverse | [bool](#bool) |  |  |
| min | [Score](#immudb.schema.Score) |  |  |
| max | [Score](#immudb.schema.Score) |  |  |





 


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
| PrintTree | [.google.protobuf.Empty](#google.protobuf.Empty) | [Tree](#immudb.schema.Tree) |  |
| Login | [LoginRequest](#immudb.schema.LoginRequest) | [LoginResponse](#immudb.schema.LoginResponse) |  |
| Logout | [.google.protobuf.Empty](#google.protobuf.Empty) | [.google.protobuf.Empty](#google.protobuf.Empty) |  |
| Set | [SetRequest](#immudb.schema.SetRequest) | [TxMetadata](#immudb.schema.TxMetadata) |  |
| VerifiableSet | [VerifiableSetRequest](#immudb.schema.VerifiableSetRequest) | [VerifiableTx](#immudb.schema.VerifiableTx) |  |
| Get | [KeyRequest](#immudb.schema.KeyRequest) | [Item](#immudb.schema.Item) |  |
| VerifiableGet | [VerifiableGetRequest](#immudb.schema.VerifiableGetRequest) | [VerifiableItem](#immudb.schema.VerifiableItem) |  |
| GetAll | [KeyListRequest](#immudb.schema.KeyListRequest) | [ItemList](#immudb.schema.ItemList) |  |
| ExecAllOps | [Ops](#immudb.schema.Ops) | [TxMetadata](#immudb.schema.TxMetadata) |  |
| Scan | [ScanRequest](#immudb.schema.ScanRequest) | [ItemList](#immudb.schema.ItemList) |  |
| Count | [KeyPrefix](#immudb.schema.KeyPrefix) | [ItemsCount](#immudb.schema.ItemsCount) |  |
| CountAll | [.google.protobuf.Empty](#google.protobuf.Empty) | [ItemsCount](#immudb.schema.ItemsCount) |  |
| TxById | [TxRequest](#immudb.schema.TxRequest) | [Tx](#immudb.schema.Tx) |  |
| VerifiableTxById | [VerifiableTxRequest](#immudb.schema.VerifiableTxRequest) | [VerifiableTx](#immudb.schema.VerifiableTx) |  |
| History | [HistoryRequest](#immudb.schema.HistoryRequest) | [ItemList](#immudb.schema.ItemList) |  |
| Health | [.google.protobuf.Empty](#google.protobuf.Empty) | [HealthResponse](#immudb.schema.HealthResponse) |  |
| CurrentState | [.google.protobuf.Empty](#google.protobuf.Empty) | [ImmutableState](#immudb.schema.ImmutableState) |  |
| SetReference | [Reference](#immudb.schema.Reference) | [TxMetadata](#immudb.schema.TxMetadata) |  |
| VerifiableSetReference | [VerifiableReferenceRequest](#immudb.schema.VerifiableReferenceRequest) | [VerifiableTx](#immudb.schema.VerifiableTx) |  |
| ZAdd | [ZAddRequest](#immudb.schema.ZAddRequest) | [TxMetadata](#immudb.schema.TxMetadata) |  |
| VerifiableZAdd | [VerifiableZAddRequest](#immudb.schema.VerifiableZAddRequest) | [VerifiableTx](#immudb.schema.VerifiableTx) |  |
| ZScan | [ZScanRequest](#immudb.schema.ZScanRequest) | [ZItemList](#immudb.schema.ZItemList) |  |
| IScan | [IScanRequest](#immudb.schema.IScanRequest) | [Page](#immudb.schema.Page) |  |
| CreateDatabase | [Database](#immudb.schema.Database) | [.google.protobuf.Empty](#google.protobuf.Empty) |  |
| UseDatabase | [Database](#immudb.schema.Database) | [UseDatabaseReply](#immudb.schema.UseDatabaseReply) |  |
| ChangePermission | [ChangePermissionRequest](#immudb.schema.ChangePermissionRequest) | [.google.protobuf.Empty](#google.protobuf.Empty) |  |
| SetActiveUser | [SetActiveUserRequest](#immudb.schema.SetActiveUserRequest) | [.google.protobuf.Empty](#google.protobuf.Empty) |  |
| DatabaseList | [.google.protobuf.Empty](#google.protobuf.Empty) | [DatabaseListResponse](#immudb.schema.DatabaseListResponse) |  |

 



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


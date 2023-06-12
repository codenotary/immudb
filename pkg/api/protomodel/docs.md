# Protocol Documentation
<a name="top"></a>

## Table of Contents

- [authorization.proto](#authorization.proto)
    - [CloseSessionRequest](#immudb.model.CloseSessionRequest)
    - [CloseSessionResponse](#immudb.model.CloseSessionResponse)
    - [KeepAliveRequest](#immudb.model.KeepAliveRequest)
    - [KeepAliveResponse](#immudb.model.KeepAliveResponse)
    - [OpenSessionRequest](#immudb.model.OpenSessionRequest)
    - [OpenSessionResponse](#immudb.model.OpenSessionResponse)
  
    - [AuthorizationService](#immudb.model.AuthorizationService)
  
- [documents.proto](#documents.proto)
    - [AuditDocumentRequest](#immudb.model.AuditDocumentRequest)
    - [AuditDocumentResponse](#immudb.model.AuditDocumentResponse)
    - [Collection](#immudb.model.Collection)
    - [CountDocumentsRequest](#immudb.model.CountDocumentsRequest)
    - [CountDocumentsResponse](#immudb.model.CountDocumentsResponse)
    - [CreateCollectionRequest](#immudb.model.CreateCollectionRequest)
    - [CreateCollectionResponse](#immudb.model.CreateCollectionResponse)
    - [CreateIndexRequest](#immudb.model.CreateIndexRequest)
    - [CreateIndexResponse](#immudb.model.CreateIndexResponse)
    - [DeleteCollectionRequest](#immudb.model.DeleteCollectionRequest)
    - [DeleteCollectionResponse](#immudb.model.DeleteCollectionResponse)
    - [DeleteDocumentsRequest](#immudb.model.DeleteDocumentsRequest)
    - [DeleteDocumentsResponse](#immudb.model.DeleteDocumentsResponse)
    - [DeleteIndexRequest](#immudb.model.DeleteIndexRequest)
    - [DeleteIndexResponse](#immudb.model.DeleteIndexResponse)
    - [DocumentAtRevision](#immudb.model.DocumentAtRevision)
    - [DocumentMetadata](#immudb.model.DocumentMetadata)
    - [Field](#immudb.model.Field)
    - [FieldComparison](#immudb.model.FieldComparison)
    - [GetCollectionRequest](#immudb.model.GetCollectionRequest)
    - [GetCollectionResponse](#immudb.model.GetCollectionResponse)
    - [GetCollectionsRequest](#immudb.model.GetCollectionsRequest)
    - [GetCollectionsResponse](#immudb.model.GetCollectionsResponse)
    - [Index](#immudb.model.Index)
    - [InsertDocumentsRequest](#immudb.model.InsertDocumentsRequest)
    - [InsertDocumentsResponse](#immudb.model.InsertDocumentsResponse)
    - [OrderByClause](#immudb.model.OrderByClause)
    - [ProofDocumentRequest](#immudb.model.ProofDocumentRequest)
    - [ProofDocumentResponse](#immudb.model.ProofDocumentResponse)
    - [Query](#immudb.model.Query)
    - [QueryExpression](#immudb.model.QueryExpression)
    - [ReplaceDocumentsRequest](#immudb.model.ReplaceDocumentsRequest)
    - [ReplaceDocumentsResponse](#immudb.model.ReplaceDocumentsResponse)
    - [SearchDocumentsRequest](#immudb.model.SearchDocumentsRequest)
    - [SearchDocumentsResponse](#immudb.model.SearchDocumentsResponse)
    - [UpdateCollectionRequest](#immudb.model.UpdateCollectionRequest)
    - [UpdateCollectionResponse](#immudb.model.UpdateCollectionResponse)
  
    - [ComparisonOperator](#immudb.model.ComparisonOperator)
    - [FieldType](#immudb.model.FieldType)
  
    - [DocumentService](#immudb.model.DocumentService)
  
- [Scalar Value Types](#scalar-value-types)



<a name="authorization.proto"></a>
<p align="right"><a href="#top">Top</a></p>

## authorization.proto



<a name="immudb.model.CloseSessionRequest"></a>

### CloseSessionRequest







<a name="immudb.model.CloseSessionResponse"></a>

### CloseSessionResponse







<a name="immudb.model.KeepAliveRequest"></a>

### KeepAliveRequest







<a name="immudb.model.KeepAliveResponse"></a>

### KeepAliveResponse







<a name="immudb.model.OpenSessionRequest"></a>

### OpenSessionRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| username | [string](#string) |  |  |
| password | [string](#string) |  |  |
| database | [string](#string) |  |  |






<a name="immudb.model.OpenSessionResponse"></a>

### OpenSessionResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| sessionID | [string](#string) |  |  |
| serverUUID | [string](#string) |  |  |
| expirationTimestamp | [int32](#int32) |  |  |
| inactivityTimestamp | [int32](#int32) |  |  |





 

 

 


<a name="immudb.model.AuthorizationService"></a>

### AuthorizationService


| Method Name | Request Type | Response Type | Description |
| ----------- | ------------ | ------------- | ------------|
| OpenSession | [OpenSessionRequest](#immudb.model.OpenSessionRequest) | [OpenSessionResponse](#immudb.model.OpenSessionResponse) |  |
| KeepAlive | [KeepAliveRequest](#immudb.model.KeepAliveRequest) | [KeepAliveResponse](#immudb.model.KeepAliveResponse) |  |
| CloseSession | [CloseSessionRequest](#immudb.model.CloseSessionRequest) | [CloseSessionResponse](#immudb.model.CloseSessionResponse) |  |

 



<a name="documents.proto"></a>
<p align="right"><a href="#top">Top</a></p>

## documents.proto



<a name="immudb.model.AuditDocumentRequest"></a>

### AuditDocumentRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| collectionName | [string](#string) |  |  |
| documentId | [string](#string) |  |  |
| desc | [bool](#bool) |  |  |
| page | [uint32](#uint32) |  |  |
| pageSize | [uint32](#uint32) |  |  |






<a name="immudb.model.AuditDocumentResponse"></a>

### AuditDocumentResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| revisions | [DocumentAtRevision](#immudb.model.DocumentAtRevision) | repeated |  |






<a name="immudb.model.Collection"></a>

### Collection



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| name | [string](#string) |  |  |
| documentIdFieldName | [string](#string) |  |  |
| fields | [Field](#immudb.model.Field) | repeated |  |
| indexes | [Index](#immudb.model.Index) | repeated |  |






<a name="immudb.model.CountDocumentsRequest"></a>

### CountDocumentsRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| query | [Query](#immudb.model.Query) |  |  |






<a name="immudb.model.CountDocumentsResponse"></a>

### CountDocumentsResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| count | [int64](#int64) |  |  |






<a name="immudb.model.CreateCollectionRequest"></a>

### CreateCollectionRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| name | [string](#string) |  |  |
| documentIdFieldName | [string](#string) |  |  |
| fields | [Field](#immudb.model.Field) | repeated |  |
| indexes | [Index](#immudb.model.Index) | repeated |  |






<a name="immudb.model.CreateCollectionResponse"></a>

### CreateCollectionResponse







<a name="immudb.model.CreateIndexRequest"></a>

### CreateIndexRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| collectionName | [string](#string) |  |  |
| fields | [string](#string) | repeated |  |
| isUnique | [bool](#bool) |  |  |






<a name="immudb.model.CreateIndexResponse"></a>

### CreateIndexResponse







<a name="immudb.model.DeleteCollectionRequest"></a>

### DeleteCollectionRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| name | [string](#string) |  |  |






<a name="immudb.model.DeleteCollectionResponse"></a>

### DeleteCollectionResponse







<a name="immudb.model.DeleteDocumentsRequest"></a>

### DeleteDocumentsRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| query | [Query](#immudb.model.Query) |  |  |






<a name="immudb.model.DeleteDocumentsResponse"></a>

### DeleteDocumentsResponse







<a name="immudb.model.DeleteIndexRequest"></a>

### DeleteIndexRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| collectionName | [string](#string) |  |  |
| fields | [string](#string) | repeated |  |






<a name="immudb.model.DeleteIndexResponse"></a>

### DeleteIndexResponse







<a name="immudb.model.DocumentAtRevision"></a>

### DocumentAtRevision



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| transactionId | [uint64](#uint64) |  |  |
| documentId | [string](#string) |  |  |
| revision | [uint64](#uint64) |  |  |
| metadata | [DocumentMetadata](#immudb.model.DocumentMetadata) |  |  |
| document | [google.protobuf.Struct](#google.protobuf.Struct) |  |  |






<a name="immudb.model.DocumentMetadata"></a>

### DocumentMetadata



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| deleted | [bool](#bool) |  |  |






<a name="immudb.model.Field"></a>

### Field



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| name | [string](#string) |  |  |
| type | [FieldType](#immudb.model.FieldType) |  |  |






<a name="immudb.model.FieldComparison"></a>

### FieldComparison



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| field | [string](#string) |  |  |
| operator | [ComparisonOperator](#immudb.model.ComparisonOperator) |  |  |
| value | [google.protobuf.Value](#google.protobuf.Value) |  |  |






<a name="immudb.model.GetCollectionRequest"></a>

### GetCollectionRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| name | [string](#string) |  |  |






<a name="immudb.model.GetCollectionResponse"></a>

### GetCollectionResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| collection | [Collection](#immudb.model.Collection) |  |  |






<a name="immudb.model.GetCollectionsRequest"></a>

### GetCollectionsRequest







<a name="immudb.model.GetCollectionsResponse"></a>

### GetCollectionsResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| collections | [Collection](#immudb.model.Collection) | repeated |  |






<a name="immudb.model.Index"></a>

### Index



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| fields | [string](#string) | repeated |  |
| isUnique | [bool](#bool) |  |  |






<a name="immudb.model.InsertDocumentsRequest"></a>

### InsertDocumentsRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| collectionName | [string](#string) |  |  |
| documents | [google.protobuf.Struct](#google.protobuf.Struct) | repeated |  |






<a name="immudb.model.InsertDocumentsResponse"></a>

### InsertDocumentsResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| transactionId | [uint64](#uint64) |  |  |
| documentIds | [string](#string) | repeated |  |






<a name="immudb.model.OrderByClause"></a>

### OrderByClause



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| field | [string](#string) |  |  |
| desc | [bool](#bool) |  |  |






<a name="immudb.model.ProofDocumentRequest"></a>

### ProofDocumentRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| collectionName | [string](#string) |  |  |
| documentId | [string](#string) |  |  |
| transactionId | [uint64](#uint64) |  |  |
| proofSinceTransactionId | [uint64](#uint64) |  |  |






<a name="immudb.model.ProofDocumentResponse"></a>

### ProofDocumentResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| database | [string](#string) |  |  |
| collectionId | [uint32](#uint32) |  |  |
| documentIdFieldName | [string](#string) |  |  |
| encodedDocument | [bytes](#bytes) |  |  |
| verifiableTx | [immudb.schema.VerifiableTxV2](#immudb.schema.VerifiableTxV2) |  |  |






<a name="immudb.model.Query"></a>

### Query



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| collectionName | [string](#string) |  |  |
| expressions | [QueryExpression](#immudb.model.QueryExpression) | repeated |  |
| orderBy | [OrderByClause](#immudb.model.OrderByClause) | repeated |  |
| limit | [uint32](#uint32) |  |  |






<a name="immudb.model.QueryExpression"></a>

### QueryExpression



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| fieldComparisons | [FieldComparison](#immudb.model.FieldComparison) | repeated |  |






<a name="immudb.model.ReplaceDocumentsRequest"></a>

### ReplaceDocumentsRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| query | [Query](#immudb.model.Query) |  |  |
| document | [google.protobuf.Struct](#google.protobuf.Struct) |  |  |






<a name="immudb.model.ReplaceDocumentsResponse"></a>

### ReplaceDocumentsResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| revisions | [DocumentAtRevision](#immudb.model.DocumentAtRevision) | repeated |  |






<a name="immudb.model.SearchDocumentsRequest"></a>

### SearchDocumentsRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| searchId | [string](#string) |  |  |
| query | [Query](#immudb.model.Query) |  |  |
| page | [uint32](#uint32) |  |  |
| pageSize | [uint32](#uint32) |  |  |
| keepOpen | [bool](#bool) |  |  |






<a name="immudb.model.SearchDocumentsResponse"></a>

### SearchDocumentsResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| searchId | [string](#string) |  |  |
| revisions | [DocumentAtRevision](#immudb.model.DocumentAtRevision) | repeated |  |






<a name="immudb.model.UpdateCollectionRequest"></a>

### UpdateCollectionRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| name | [string](#string) |  |  |
| documentIdFieldName | [string](#string) |  |  |






<a name="immudb.model.UpdateCollectionResponse"></a>

### UpdateCollectionResponse






 


<a name="immudb.model.ComparisonOperator"></a>

### ComparisonOperator


| Name | Number | Description |
| ---- | ------ | ----------- |
| EQ | 0 |  |
| NE | 1 |  |
| LT | 2 |  |
| LE | 3 |  |
| GT | 4 |  |
| GE | 5 |  |
| LIKE | 6 |  |
| NOT_LIKE | 7 |  |



<a name="immudb.model.FieldType"></a>

### FieldType


| Name | Number | Description |
| ---- | ------ | ----------- |
| STRING | 0 |  |
| BOOLEAN | 1 |  |
| INTEGER | 2 |  |
| DOUBLE | 3 |  |


 

 


<a name="immudb.model.DocumentService"></a>

### DocumentService


| Method Name | Request Type | Response Type | Description |
| ----------- | ------------ | ------------- | ------------|
| CreateCollection | [CreateCollectionRequest](#immudb.model.CreateCollectionRequest) | [CreateCollectionResponse](#immudb.model.CreateCollectionResponse) |  |
| GetCollections | [GetCollectionsRequest](#immudb.model.GetCollectionsRequest) | [GetCollectionsResponse](#immudb.model.GetCollectionsResponse) |  |
| GetCollection | [GetCollectionRequest](#immudb.model.GetCollectionRequest) | [GetCollectionResponse](#immudb.model.GetCollectionResponse) |  |
| UpdateCollection | [UpdateCollectionRequest](#immudb.model.UpdateCollectionRequest) | [UpdateCollectionResponse](#immudb.model.UpdateCollectionResponse) |  |
| DeleteCollection | [DeleteCollectionRequest](#immudb.model.DeleteCollectionRequest) | [DeleteCollectionResponse](#immudb.model.DeleteCollectionResponse) |  |
| CreateIndex | [CreateIndexRequest](#immudb.model.CreateIndexRequest) | [CreateIndexResponse](#immudb.model.CreateIndexResponse) |  |
| DeleteIndex | [DeleteIndexRequest](#immudb.model.DeleteIndexRequest) | [DeleteIndexResponse](#immudb.model.DeleteIndexResponse) |  |
| InsertDocuments | [InsertDocumentsRequest](#immudb.model.InsertDocumentsRequest) | [InsertDocumentsResponse](#immudb.model.InsertDocumentsResponse) |  |
| ReplaceDocuments | [ReplaceDocumentsRequest](#immudb.model.ReplaceDocumentsRequest) | [ReplaceDocumentsResponse](#immudb.model.ReplaceDocumentsResponse) |  |
| DeleteDocuments | [DeleteDocumentsRequest](#immudb.model.DeleteDocumentsRequest) | [DeleteDocumentsResponse](#immudb.model.DeleteDocumentsResponse) |  |
| SearchDocuments | [SearchDocumentsRequest](#immudb.model.SearchDocumentsRequest) | [SearchDocumentsResponse](#immudb.model.SearchDocumentsResponse) |  |
| CountDocuments | [CountDocumentsRequest](#immudb.model.CountDocumentsRequest) | [CountDocumentsResponse](#immudb.model.CountDocumentsResponse) |  |
| AuditDocument | [AuditDocumentRequest](#immudb.model.AuditDocumentRequest) | [AuditDocumentResponse](#immudb.model.AuditDocumentResponse) |  |
| ProofDocument | [ProofDocumentRequest](#immudb.model.ProofDocumentRequest) | [ProofDocumentResponse](#immudb.model.ProofDocumentResponse) |  |

 



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


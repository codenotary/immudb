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
    - [Collection](#immudb.model.Collection)
    - [CollectionCreateRequest](#immudb.model.CollectionCreateRequest)
    - [CollectionCreateResponse](#immudb.model.CollectionCreateResponse)
    - [CollectionDeleteRequest](#immudb.model.CollectionDeleteRequest)
    - [CollectionDeleteResponse](#immudb.model.CollectionDeleteResponse)
    - [CollectionGetRequest](#immudb.model.CollectionGetRequest)
    - [CollectionGetResponse](#immudb.model.CollectionGetResponse)
    - [CollectionListRequest](#immudb.model.CollectionListRequest)
    - [CollectionListResponse](#immudb.model.CollectionListResponse)
    - [CollectionUpdateRequest](#immudb.model.CollectionUpdateRequest)
    - [CollectionUpdateResponse](#immudb.model.CollectionUpdateResponse)
    - [DocumentAtRevision](#immudb.model.DocumentAtRevision)
    - [DocumentAuditRequest](#immudb.model.DocumentAuditRequest)
    - [DocumentAuditResponse](#immudb.model.DocumentAuditResponse)
    - [DocumentDeleteRequest](#immudb.model.DocumentDeleteRequest)
    - [DocumentDeleteResponse](#immudb.model.DocumentDeleteResponse)
    - [DocumentInsertManyRequest](#immudb.model.DocumentInsertManyRequest)
    - [DocumentInsertManyResponse](#immudb.model.DocumentInsertManyResponse)
    - [DocumentInsertRequest](#immudb.model.DocumentInsertRequest)
    - [DocumentInsertResponse](#immudb.model.DocumentInsertResponse)
    - [DocumentMetadata](#immudb.model.DocumentMetadata)
    - [DocumentProofRequest](#immudb.model.DocumentProofRequest)
    - [DocumentProofResponse](#immudb.model.DocumentProofResponse)
    - [DocumentSearchRequest](#immudb.model.DocumentSearchRequest)
    - [DocumentSearchResponse](#immudb.model.DocumentSearchResponse)
    - [DocumentUpdateRequest](#immudb.model.DocumentUpdateRequest)
    - [DocumentUpdateResponse](#immudb.model.DocumentUpdateResponse)
    - [Field](#immudb.model.Field)
    - [FieldComparison](#immudb.model.FieldComparison)
    - [Index](#immudb.model.Index)
    - [IndexCreateRequest](#immudb.model.IndexCreateRequest)
    - [IndexCreateResponse](#immudb.model.IndexCreateResponse)
    - [IndexDeleteRequest](#immudb.model.IndexDeleteRequest)
    - [IndexDeleteResponse](#immudb.model.IndexDeleteResponse)
    - [OrderByClause](#immudb.model.OrderByClause)
    - [Query](#immudb.model.Query)
    - [QueryExpression](#immudb.model.QueryExpression)
  
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



<a name="immudb.model.Collection"></a>

### Collection



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| name | [string](#string) |  |  |
| idFieldName | [string](#string) |  |  |
| fields | [Field](#immudb.model.Field) | repeated |  |
| indexes | [Index](#immudb.model.Index) | repeated |  |






<a name="immudb.model.CollectionCreateRequest"></a>

### CollectionCreateRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| name | [string](#string) |  |  |
| idFieldName | [string](#string) |  |  |
| fields | [Field](#immudb.model.Field) | repeated |  |
| indexes | [Index](#immudb.model.Index) | repeated |  |






<a name="immudb.model.CollectionCreateResponse"></a>

### CollectionCreateResponse







<a name="immudb.model.CollectionDeleteRequest"></a>

### CollectionDeleteRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| name | [string](#string) |  |  |






<a name="immudb.model.CollectionDeleteResponse"></a>

### CollectionDeleteResponse







<a name="immudb.model.CollectionGetRequest"></a>

### CollectionGetRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| name | [string](#string) |  |  |






<a name="immudb.model.CollectionGetResponse"></a>

### CollectionGetResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| collection | [Collection](#immudb.model.Collection) |  |  |






<a name="immudb.model.CollectionListRequest"></a>

### CollectionListRequest







<a name="immudb.model.CollectionListResponse"></a>

### CollectionListResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| collections | [Collection](#immudb.model.Collection) | repeated |  |






<a name="immudb.model.CollectionUpdateRequest"></a>

### CollectionUpdateRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| name | [string](#string) |  |  |
| idFieldName | [string](#string) |  |  |






<a name="immudb.model.CollectionUpdateResponse"></a>

### CollectionUpdateResponse







<a name="immudb.model.DocumentAtRevision"></a>

### DocumentAtRevision



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| transactionId | [uint64](#uint64) |  |  |
| revision | [uint64](#uint64) |  |  |
| metadata | [DocumentMetadata](#immudb.model.DocumentMetadata) |  |  |
| document | [google.protobuf.Struct](#google.protobuf.Struct) |  |  |






<a name="immudb.model.DocumentAuditRequest"></a>

### DocumentAuditRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| collection | [string](#string) |  |  |
| documentId | [string](#string) |  |  |
| desc | [bool](#bool) |  |  |
| page | [uint32](#uint32) |  |  |
| pageSize | [uint32](#uint32) |  |  |






<a name="immudb.model.DocumentAuditResponse"></a>

### DocumentAuditResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| revisions | [DocumentAtRevision](#immudb.model.DocumentAtRevision) | repeated |  |






<a name="immudb.model.DocumentDeleteRequest"></a>

### DocumentDeleteRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| query | [Query](#immudb.model.Query) |  |  |






<a name="immudb.model.DocumentDeleteResponse"></a>

### DocumentDeleteResponse







<a name="immudb.model.DocumentInsertManyRequest"></a>

### DocumentInsertManyRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| collection | [string](#string) |  |  |
| documents | [google.protobuf.Struct](#google.protobuf.Struct) | repeated |  |






<a name="immudb.model.DocumentInsertManyResponse"></a>

### DocumentInsertManyResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| transactionId | [uint64](#uint64) |  |  |
| documentIds | [string](#string) | repeated |  |






<a name="immudb.model.DocumentInsertRequest"></a>

### DocumentInsertRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| collection | [string](#string) |  |  |
| document | [google.protobuf.Struct](#google.protobuf.Struct) |  |  |






<a name="immudb.model.DocumentInsertResponse"></a>

### DocumentInsertResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| transactionId | [uint64](#uint64) |  |  |
| documentId | [string](#string) |  |  |






<a name="immudb.model.DocumentMetadata"></a>

### DocumentMetadata



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| deleted | [bool](#bool) |  |  |






<a name="immudb.model.DocumentProofRequest"></a>

### DocumentProofRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| collection | [string](#string) |  |  |
| documentId | [string](#string) |  |  |
| transactionId | [uint64](#uint64) |  |  |
| proofSinceTransactionId | [uint64](#uint64) |  |  |






<a name="immudb.model.DocumentProofResponse"></a>

### DocumentProofResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| database | [string](#string) |  |  |
| collectionId | [uint32](#uint32) |  |  |
| idFieldName | [string](#string) |  |  |
| encodedDocument | [bytes](#bytes) |  |  |
| verifiableTx | [immudb.schema.VerifiableTxV2](#immudb.schema.VerifiableTxV2) |  |  |






<a name="immudb.model.DocumentSearchRequest"></a>

### DocumentSearchRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| searchId | [string](#string) |  |  |
| query | [Query](#immudb.model.Query) |  |  |
| page | [uint32](#uint32) |  |  |
| pageSize | [uint32](#uint32) |  |  |






<a name="immudb.model.DocumentSearchResponse"></a>

### DocumentSearchResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| searchId | [string](#string) |  |  |
| revisions | [DocumentAtRevision](#immudb.model.DocumentAtRevision) | repeated |  |






<a name="immudb.model.DocumentUpdateRequest"></a>

### DocumentUpdateRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| query | [Query](#immudb.model.Query) |  |  |
| document | [google.protobuf.Struct](#google.protobuf.Struct) |  |  |






<a name="immudb.model.DocumentUpdateResponse"></a>

### DocumentUpdateResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| transactionId | [uint64](#uint64) |  |  |
| documentId | [string](#string) |  |  |
| revision | [uint64](#uint64) |  |  |






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






<a name="immudb.model.Index"></a>

### Index



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| fields | [string](#string) | repeated |  |
| isUnique | [bool](#bool) |  |  |






<a name="immudb.model.IndexCreateRequest"></a>

### IndexCreateRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| collection | [string](#string) |  |  |
| fields | [string](#string) | repeated |  |
| isUnique | [bool](#bool) |  |  |






<a name="immudb.model.IndexCreateResponse"></a>

### IndexCreateResponse







<a name="immudb.model.IndexDeleteRequest"></a>

### IndexDeleteRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| collection | [string](#string) |  |  |
| fields | [string](#string) | repeated |  |






<a name="immudb.model.IndexDeleteResponse"></a>

### IndexDeleteResponse







<a name="immudb.model.OrderByClause"></a>

### OrderByClause



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| field | [string](#string) |  |  |
| desc | [bool](#bool) |  |  |






<a name="immudb.model.Query"></a>

### Query



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| collection | [string](#string) |  |  |
| expressions | [QueryExpression](#immudb.model.QueryExpression) | repeated |  |
| orderBy | [OrderByClause](#immudb.model.OrderByClause) | repeated |  |






<a name="immudb.model.QueryExpression"></a>

### QueryExpression



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| fieldComparisons | [FieldComparison](#immudb.model.FieldComparison) | repeated |  |





 


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
| CollectionCreate | [CollectionCreateRequest](#immudb.model.CollectionCreateRequest) | [CollectionCreateResponse](#immudb.model.CollectionCreateResponse) |  |
| CollectionGet | [CollectionGetRequest](#immudb.model.CollectionGetRequest) | [CollectionGetResponse](#immudb.model.CollectionGetResponse) |  |
| CollectionList | [CollectionListRequest](#immudb.model.CollectionListRequest) | [CollectionListResponse](#immudb.model.CollectionListResponse) |  |
| CollectionDelete | [CollectionDeleteRequest](#immudb.model.CollectionDeleteRequest) | [CollectionDeleteResponse](#immudb.model.CollectionDeleteResponse) |  |
| CollectionUpdate | [CollectionUpdateRequest](#immudb.model.CollectionUpdateRequest) | [CollectionUpdateResponse](#immudb.model.CollectionUpdateResponse) |  |
| IndexCreate | [IndexCreateRequest](#immudb.model.IndexCreateRequest) | [IndexCreateResponse](#immudb.model.IndexCreateResponse) |  |
| IndexDelete | [IndexDeleteRequest](#immudb.model.IndexDeleteRequest) | [IndexDeleteResponse](#immudb.model.IndexDeleteResponse) |  |
| DocumentInsert | [DocumentInsertRequest](#immudb.model.DocumentInsertRequest) | [DocumentInsertResponse](#immudb.model.DocumentInsertResponse) |  |
| DocumentInsertMany | [DocumentInsertManyRequest](#immudb.model.DocumentInsertManyRequest) | [DocumentInsertManyResponse](#immudb.model.DocumentInsertManyResponse) |  |
| DocumentUpdate | [DocumentUpdateRequest](#immudb.model.DocumentUpdateRequest) | [DocumentUpdateResponse](#immudb.model.DocumentUpdateResponse) |  |
| DocumentSearch | [DocumentSearchRequest](#immudb.model.DocumentSearchRequest) | [DocumentSearchResponse](#immudb.model.DocumentSearchResponse) |  |
| DocumentAudit | [DocumentAuditRequest](#immudb.model.DocumentAuditRequest) | [DocumentAuditResponse](#immudb.model.DocumentAuditResponse) |  |
| DocumentProof | [DocumentProofRequest](#immudb.model.DocumentProofRequest) | [DocumentProofResponse](#immudb.model.DocumentProofResponse) |  |
| DocumentDelete | [DocumentDeleteRequest](#immudb.model.DocumentDeleteRequest) | [DocumentDeleteResponse](#immudb.model.DocumentDeleteResponse) |  |

 



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


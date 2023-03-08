# Protocol Documentation
<a name="top"></a>

## Table of Contents

- [documentsschema.proto](#documentsschema.proto)
    - [CollectionCreateRequest](#immudb.documentsschema.CollectionCreateRequest)
    - [CollectionCreateRequest.IndexKeysEntry](#immudb.documentsschema.CollectionCreateRequest.IndexKeysEntry)
    - [CollectionCreateRequest.PrimaryKeysEntry](#immudb.documentsschema.CollectionCreateRequest.PrimaryKeysEntry)
    - [CollectionCreateResponse](#immudb.documentsschema.CollectionCreateResponse)
    - [CollectionDeleteRequest](#immudb.documentsschema.CollectionDeleteRequest)
    - [CollectionDeleteResponse](#immudb.documentsschema.CollectionDeleteResponse)
    - [CollectionGetRequest](#immudb.documentsschema.CollectionGetRequest)
    - [CollectionGetResponse](#immudb.documentsschema.CollectionGetResponse)
    - [CollectionInformation](#immudb.documentsschema.CollectionInformation)
    - [CollectionInformation.IndexKeysEntry](#immudb.documentsschema.CollectionInformation.IndexKeysEntry)
    - [CollectionInformation.PrimaryKeysEntry](#immudb.documentsschema.CollectionInformation.PrimaryKeysEntry)
    - [CollectionListRequest](#immudb.documentsschema.CollectionListRequest)
    - [CollectionListResponse](#immudb.documentsschema.CollectionListResponse)
    - [DocumentAudit](#immudb.documentsschema.DocumentAudit)
    - [DocumentAuditRequest](#immudb.documentsschema.DocumentAuditRequest)
    - [DocumentAuditRequest.PrimaryKeysEntry](#immudb.documentsschema.DocumentAuditRequest.PrimaryKeysEntry)
    - [DocumentAuditResponse](#immudb.documentsschema.DocumentAuditResponse)
    - [DocumentInsertRequest](#immudb.documentsschema.DocumentInsertRequest)
    - [DocumentInsertResponse](#immudb.documentsschema.DocumentInsertResponse)
    - [DocumentProofRequest](#immudb.documentsschema.DocumentProofRequest)
    - [DocumentProofRequest.PrimaryKeysEntry](#immudb.documentsschema.DocumentProofRequest.PrimaryKeysEntry)
    - [DocumentProofResponse](#immudb.documentsschema.DocumentProofResponse)
    - [DocumentQuery](#immudb.documentsschema.DocumentQuery)
    - [DocumentSearchRequest](#immudb.documentsschema.DocumentSearchRequest)
    - [DocumentSearchResponse](#immudb.documentsschema.DocumentSearchResponse)
    - [IndexOption](#immudb.documentsschema.IndexOption)
    - [IndexValue](#immudb.documentsschema.IndexValue)
    - [Proof](#immudb.documentsschema.Proof)
  
    - [IndexType](#immudb.documentsschema.IndexType)
    - [QueryOperator](#immudb.documentsschema.QueryOperator)
  
    - [DocumentService](#immudb.documentsschema.DocumentService)
  
- [Scalar Value Types](#scalar-value-types)



<a name="documentsschema.proto"></a>
<p align="right"><a href="#top">Top</a></p>

## documentsschema.proto



<a name="immudb.documentsschema.CollectionCreateRequest"></a>

### CollectionCreateRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| name | [string](#string) |  |  |
| primaryKeys | [CollectionCreateRequest.PrimaryKeysEntry](#immudb.documentsschema.CollectionCreateRequest.PrimaryKeysEntry) | repeated |  |
| indexKeys | [CollectionCreateRequest.IndexKeysEntry](#immudb.documentsschema.CollectionCreateRequest.IndexKeysEntry) | repeated |  |






<a name="immudb.documentsschema.CollectionCreateRequest.IndexKeysEntry"></a>

### CollectionCreateRequest.IndexKeysEntry



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| key | [string](#string) |  |  |
| value | [IndexOption](#immudb.documentsschema.IndexOption) |  |  |






<a name="immudb.documentsschema.CollectionCreateRequest.PrimaryKeysEntry"></a>

### CollectionCreateRequest.PrimaryKeysEntry



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| key | [string](#string) |  |  |
| value | [IndexOption](#immudb.documentsschema.IndexOption) |  |  |






<a name="immudb.documentsschema.CollectionCreateResponse"></a>

### CollectionCreateResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| collection | [CollectionInformation](#immudb.documentsschema.CollectionInformation) |  |  |






<a name="immudb.documentsschema.CollectionDeleteRequest"></a>

### CollectionDeleteRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| name | [string](#string) |  |  |






<a name="immudb.documentsschema.CollectionDeleteResponse"></a>

### CollectionDeleteResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| response | [google.protobuf.Empty](#google.protobuf.Empty) |  |  |






<a name="immudb.documentsschema.CollectionGetRequest"></a>

### CollectionGetRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| name | [string](#string) |  |  |






<a name="immudb.documentsschema.CollectionGetResponse"></a>

### CollectionGetResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| collection | [CollectionInformation](#immudb.documentsschema.CollectionInformation) |  |  |






<a name="immudb.documentsschema.CollectionInformation"></a>

### CollectionInformation



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| name | [string](#string) |  |  |
| primaryKeys | [CollectionInformation.PrimaryKeysEntry](#immudb.documentsschema.CollectionInformation.PrimaryKeysEntry) | repeated |  |
| indexKeys | [CollectionInformation.IndexKeysEntry](#immudb.documentsschema.CollectionInformation.IndexKeysEntry) | repeated |  |






<a name="immudb.documentsschema.CollectionInformation.IndexKeysEntry"></a>

### CollectionInformation.IndexKeysEntry



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| key | [string](#string) |  |  |
| value | [IndexOption](#immudb.documentsschema.IndexOption) |  |  |






<a name="immudb.documentsschema.CollectionInformation.PrimaryKeysEntry"></a>

### CollectionInformation.PrimaryKeysEntry



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| key | [string](#string) |  |  |
| value | [IndexOption](#immudb.documentsschema.IndexOption) |  |  |






<a name="immudb.documentsschema.CollectionListRequest"></a>

### CollectionListRequest







<a name="immudb.documentsschema.CollectionListResponse"></a>

### CollectionListResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| collections | [CollectionInformation](#immudb.documentsschema.CollectionInformation) | repeated |  |






<a name="immudb.documentsschema.DocumentAudit"></a>

### DocumentAudit



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| value | [google.protobuf.Struct](#google.protobuf.Struct) |  |  |
| transactionID | [uint64](#uint64) |  |  |






<a name="immudb.documentsschema.DocumentAuditRequest"></a>

### DocumentAuditRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| collection | [string](#string) |  |  |
| primaryKeys | [DocumentAuditRequest.PrimaryKeysEntry](#immudb.documentsschema.DocumentAuditRequest.PrimaryKeysEntry) | repeated |  |
| page | [uint32](#uint32) |  |  |
| perPage | [uint32](#uint32) |  |  |






<a name="immudb.documentsschema.DocumentAuditRequest.PrimaryKeysEntry"></a>

### DocumentAuditRequest.PrimaryKeysEntry



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| key | [string](#string) |  |  |
| value | [IndexValue](#immudb.documentsschema.IndexValue) |  |  |






<a name="immudb.documentsschema.DocumentAuditResponse"></a>

### DocumentAuditResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| results | [DocumentAudit](#immudb.documentsschema.DocumentAudit) | repeated |  |
| page | [uint32](#uint32) |  |  |
| perPage | [uint32](#uint32) |  |  |
| entriesLeft | [uint32](#uint32) |  |  |






<a name="immudb.documentsschema.DocumentInsertRequest"></a>

### DocumentInsertRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| collection | [string](#string) |  |  |
| document | [google.protobuf.Struct](#google.protobuf.Struct) | repeated |  |






<a name="immudb.documentsschema.DocumentInsertResponse"></a>

### DocumentInsertResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| proof | [immudb.schema.VerifiableTx](#immudb.schema.VerifiableTx) |  |  |






<a name="immudb.documentsschema.DocumentProofRequest"></a>

### DocumentProofRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| collection | [string](#string) |  |  |
| primaryKeys | [DocumentProofRequest.PrimaryKeysEntry](#immudb.documentsschema.DocumentProofRequest.PrimaryKeysEntry) | repeated |  |
| atRevision | [int64](#int64) |  |  |






<a name="immudb.documentsschema.DocumentProofRequest.PrimaryKeysEntry"></a>

### DocumentProofRequest.PrimaryKeysEntry



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| key | [string](#string) |  |  |
| value | [IndexValue](#immudb.documentsschema.IndexValue) |  |  |






<a name="immudb.documentsschema.DocumentProofResponse"></a>

### DocumentProofResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| proof | [immudb.schema.VerifiableTx](#immudb.schema.VerifiableTx) |  |  |






<a name="immudb.documentsschema.DocumentQuery"></a>

### DocumentQuery



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| field | [string](#string) |  |  |
| operator | [QueryOperator](#immudb.documentsschema.QueryOperator) |  |  |
| value | [google.protobuf.Value](#google.protobuf.Value) |  |  |






<a name="immudb.documentsschema.DocumentSearchRequest"></a>

### DocumentSearchRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| collection | [string](#string) |  |  |
| query | [DocumentQuery](#immudb.documentsschema.DocumentQuery) | repeated |  |
| page | [uint32](#uint32) |  |  |
| perPage | [uint32](#uint32) |  |  |






<a name="immudb.documentsschema.DocumentSearchResponse"></a>

### DocumentSearchResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| results | [google.protobuf.Struct](#google.protobuf.Struct) | repeated |  |
| page | [uint32](#uint32) |  |  |
| perPage | [uint32](#uint32) |  |  |
| entriesLeft | [uint32](#uint32) |  |  |






<a name="immudb.documentsschema.IndexOption"></a>

### IndexOption



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| type | [IndexType](#immudb.documentsschema.IndexType) |  |  |






<a name="immudb.documentsschema.IndexValue"></a>

### IndexValue



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| null_value | [google.protobuf.NullValue](#google.protobuf.NullValue) |  |  |
| number_value | [double](#double) |  |  |
| string_value | [string](#string) |  |  |
| bool_value | [bool](#bool) |  |  |






<a name="immudb.documentsschema.Proof"></a>

### Proof



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| name | [string](#string) |  |  |





 


<a name="immudb.documentsschema.IndexType"></a>

### IndexType


| Name | Number | Description |
| ---- | ------ | ----------- |
| DOUBLE | 0 |  |
| INTEGER | 1 |  |
| STRING | 2 |  |



<a name="immudb.documentsschema.QueryOperator"></a>

### QueryOperator


| Name | Number | Description |
| ---- | ------ | ----------- |
| EQ | 0 |  |
| GT | 1 |  |
| GTE | 2 |  |
| LT | 3 |  |
| LTE | 4 |  |
| LIKE | 5 |  |


 

 


<a name="immudb.documentsschema.DocumentService"></a>

### DocumentService


| Method Name | Request Type | Response Type | Description |
| ----------- | ------------ | ------------- | ------------|
| DocumentInsert | [DocumentInsertRequest](#immudb.documentsschema.DocumentInsertRequest) | [DocumentInsertResponse](#immudb.documentsschema.DocumentInsertResponse) |  |
| DocumentSearch | [DocumentSearchRequest](#immudb.documentsschema.DocumentSearchRequest) | [DocumentSearchResponse](#immudb.documentsschema.DocumentSearchResponse) |  |
| DocumentAudit | [DocumentAuditRequest](#immudb.documentsschema.DocumentAuditRequest) | [DocumentAuditResponse](#immudb.documentsschema.DocumentAuditResponse) |  |
| DocumentProof | [DocumentProofRequest](#immudb.documentsschema.DocumentProofRequest) | [DocumentProofResponse](#immudb.documentsschema.DocumentProofResponse) |  |
| CollectionCreate | [CollectionCreateRequest](#immudb.documentsschema.CollectionCreateRequest) | [CollectionCreateResponse](#immudb.documentsschema.CollectionCreateResponse) |  |
| CollectionGet | [CollectionGetRequest](#immudb.documentsschema.CollectionGetRequest) | [CollectionGetResponse](#immudb.documentsschema.CollectionGetResponse) |  |
| CollectionList | [CollectionListRequest](#immudb.documentsschema.CollectionListRequest) | [CollectionListResponse](#immudb.documentsschema.CollectionListResponse) |  |
| CollectionDelete | [CollectionDeleteRequest](#immudb.documentsschema.CollectionDeleteRequest) | [CollectionDeleteResponse](#immudb.documentsschema.CollectionDeleteResponse) |  |

 



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


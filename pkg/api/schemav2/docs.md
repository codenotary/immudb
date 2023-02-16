# Protocol Documentation
<a name="top"></a>

## Table of Contents

- [schemav2.proto](#schemav2.proto)
    - [CollectionCreateRequest](#immudb.schemav2.CollectionCreateRequest)
    - [CollectionCreateRequest.IndexKeysEntry](#immudb.schemav2.CollectionCreateRequest.IndexKeysEntry)
    - [CollectionCreateRequest.PrimaryKeysEntry](#immudb.schemav2.CollectionCreateRequest.PrimaryKeysEntry)
    - [CollectionDeleteRequest](#immudb.schemav2.CollectionDeleteRequest)
    - [CollectionInformation](#immudb.schemav2.CollectionInformation)
    - [CollectionInformation.IndexKeysEntry](#immudb.schemav2.CollectionInformation.IndexKeysEntry)
    - [CollectionInformation.PrimaryKeysEntry](#immudb.schemav2.CollectionInformation.PrimaryKeysEntry)
    - [CollectionListRequest](#immudb.schemav2.CollectionListRequest)
    - [CollectionListResponse](#immudb.schemav2.CollectionListResponse)
    - [DocumentInsertRequest](#immudb.schemav2.DocumentInsertRequest)
    - [DocumentSearchRequest](#immudb.schemav2.DocumentSearchRequest)
    - [DocumentSearchRequest.QueryEntry](#immudb.schemav2.DocumentSearchRequest.QueryEntry)
    - [DocumentSearchResponse](#immudb.schemav2.DocumentSearchResponse)
    - [LoginRequest](#immudb.schemav2.LoginRequest)
    - [LoginResponse](#immudb.schemav2.LoginResponse)
    - [PossibleIndexValue](#immudb.schemav2.PossibleIndexValue)
  
    - [ImmuService](#immudb.schemav2.ImmuService)
  
- [Scalar Value Types](#scalar-value-types)



<a name="schemav2.proto"></a>
<p align="right"><a href="#top">Top</a></p>

## schemav2.proto



<a name="immudb.schemav2.CollectionCreateRequest"></a>

### CollectionCreateRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| name | [string](#string) |  |  |
| primaryKeys | [CollectionCreateRequest.PrimaryKeysEntry](#immudb.schemav2.CollectionCreateRequest.PrimaryKeysEntry) | repeated |  |
| indexKeys | [CollectionCreateRequest.IndexKeysEntry](#immudb.schemav2.CollectionCreateRequest.IndexKeysEntry) | repeated |  |






<a name="immudb.schemav2.CollectionCreateRequest.IndexKeysEntry"></a>

### CollectionCreateRequest.IndexKeysEntry



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| key | [string](#string) |  |  |
| value | [PossibleIndexValue](#immudb.schemav2.PossibleIndexValue) |  |  |






<a name="immudb.schemav2.CollectionCreateRequest.PrimaryKeysEntry"></a>

### CollectionCreateRequest.PrimaryKeysEntry



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| key | [string](#string) |  |  |
| value | [PossibleIndexValue](#immudb.schemav2.PossibleIndexValue) |  |  |






<a name="immudb.schemav2.CollectionDeleteRequest"></a>

### CollectionDeleteRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| name | [string](#string) |  |  |






<a name="immudb.schemav2.CollectionInformation"></a>

### CollectionInformation



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| name | [string](#string) |  |  |
| primaryKeys | [CollectionInformation.PrimaryKeysEntry](#immudb.schemav2.CollectionInformation.PrimaryKeysEntry) | repeated |  |
| indexKeys | [CollectionInformation.IndexKeysEntry](#immudb.schemav2.CollectionInformation.IndexKeysEntry) | repeated |  |






<a name="immudb.schemav2.CollectionInformation.IndexKeysEntry"></a>

### CollectionInformation.IndexKeysEntry



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| key | [string](#string) |  |  |
| value | [PossibleIndexValue](#immudb.schemav2.PossibleIndexValue) |  |  |






<a name="immudb.schemav2.CollectionInformation.PrimaryKeysEntry"></a>

### CollectionInformation.PrimaryKeysEntry



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| key | [string](#string) |  |  |
| value | [PossibleIndexValue](#immudb.schemav2.PossibleIndexValue) |  |  |






<a name="immudb.schemav2.CollectionListRequest"></a>

### CollectionListRequest







<a name="immudb.schemav2.CollectionListResponse"></a>

### CollectionListResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| collections | [CollectionInformation](#immudb.schemav2.CollectionInformation) | repeated |  |






<a name="immudb.schemav2.DocumentInsertRequest"></a>

### DocumentInsertRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| collection | [string](#string) |  |  |
| document | [google.protobuf.Struct](#google.protobuf.Struct) | repeated |  |






<a name="immudb.schemav2.DocumentSearchRequest"></a>

### DocumentSearchRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| collection | [string](#string) |  |  |
| query | [DocumentSearchRequest.QueryEntry](#immudb.schemav2.DocumentSearchRequest.QueryEntry) | repeated |  |
| page | [uint32](#uint32) |  |  |
| perPage | [uint32](#uint32) |  |  |






<a name="immudb.schemav2.DocumentSearchRequest.QueryEntry"></a>

### DocumentSearchRequest.QueryEntry



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| key | [string](#string) |  |  |
| value | [google.protobuf.Value](#google.protobuf.Value) |  |  |






<a name="immudb.schemav2.DocumentSearchResponse"></a>

### DocumentSearchResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| results | [google.protobuf.Struct](#google.protobuf.Struct) | repeated |  |
| page | [uint32](#uint32) |  |  |
| perPage | [uint32](#uint32) |  |  |
| entriesLeft | [uint32](#uint32) |  |  |






<a name="immudb.schemav2.LoginRequest"></a>

### LoginRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| username | [string](#string) |  |  |
| password | [string](#string) |  |  |
| database | [string](#string) |  |  |
| writeAccess | [bool](#bool) |  |  |






<a name="immudb.schemav2.LoginResponse"></a>

### LoginResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| token | [string](#string) |  |  |






<a name="immudb.schemav2.PossibleIndexValue"></a>

### PossibleIndexValue



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| number_value | [double](#double) |  | Represents a double value. |
| string_value | [string](#string) |  | Represents a string value. |
| bool_value | [bool](#bool) |  | Represents a boolean value. |
| int_value | [int64](#int64) |  | Represents a int64 value |





 

 

 


<a name="immudb.schemav2.ImmuService"></a>

### ImmuService


| Method Name | Request Type | Response Type | Description |
| ----------- | ------------ | ------------- | ------------|
| Login | [LoginRequest](#immudb.schemav2.LoginRequest) | [LoginResponse](#immudb.schemav2.LoginResponse) |  |
| DocumentInsert | [DocumentInsertRequest](#immudb.schemav2.DocumentInsertRequest) | [.immudb.schema.VerifiableTx](#immudb.schema.VerifiableTx) |  |
| DocumentSearch | [DocumentSearchRequest](#immudb.schemav2.DocumentSearchRequest) | [DocumentSearchResponse](#immudb.schemav2.DocumentSearchResponse) |  |
| CollectionCreate | [CollectionCreateRequest](#immudb.schemav2.CollectionCreateRequest) | [CollectionInformation](#immudb.schemav2.CollectionInformation) |  |
| CollectionList | [CollectionListRequest](#immudb.schemav2.CollectionListRequest) | [CollectionListResponse](#immudb.schemav2.CollectionListResponse) |  |
| CollectionDelete | [CollectionDeleteRequest](#immudb.schemav2.CollectionDeleteRequest) | [.google.protobuf.Empty](#google.protobuf.Empty) |  |

 



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


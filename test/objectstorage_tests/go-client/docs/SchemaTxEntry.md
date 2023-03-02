# SchemaTxEntry

## Properties
Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**Key** | **string** |  | [optional] [default to null]
**HValue** | **string** |  | [optional] [default to null]
**VLen** | **int32** |  | [optional] [default to null]
**Metadata** | [***SchemaKvMetadata**](schemaKVMetadata.md) |  | [optional] [default to null]
**Value** | **string** | value, must be ignored when len(value) &#x3D;&#x3D; 0 and vLen &gt; 0. Otherwise sha256(value) must be equal to hValue. | [optional] [default to null]

[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)



# \DocumentsApi

All URIs are relative to *https://localhost*

Method | HTTP request | Description
------------- | ------------- | -------------
[**ImmuServiceV2DocumentInsert**](DocumentsApi.md#ImmuServiceV2DocumentInsert) | **Put** /api/documents/insert | 
[**ImmuServiceV2DocumentSearch**](DocumentsApi.md#ImmuServiceV2DocumentSearch) | **Post** /api/documents/search | 


# **ImmuServiceV2DocumentInsert**
> SchemaProof ImmuServiceV2DocumentInsert(ctx, body)


### Required Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **ctx** | **context.Context** | context for authentication, logging, cancellation, deadlines, tracing, etc.
  **body** | [**SchemaDocumentInsertRequest**](SchemaDocumentInsertRequest.md)|  | 

### Return type

[**SchemaProof**](schemaProof.md)

### Authorization

[bearer](../README.md#bearer)

### HTTP request headers

 - **Content-Type**: application/json
 - **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **ImmuServiceV2DocumentSearch**
> SchemaDocumentSearchResponse ImmuServiceV2DocumentSearch(ctx, body)


### Required Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **ctx** | **context.Context** | context for authentication, logging, cancellation, deadlines, tracing, etc.
  **body** | [**SchemaDocumentSearchRequest**](SchemaDocumentSearchRequest.md)|  | 

### Return type

[**SchemaDocumentSearchResponse**](schemaDocumentSearchResponse.md)

### Authorization

[bearer](../README.md#bearer)

### HTTP request headers

 - **Content-Type**: application/json
 - **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


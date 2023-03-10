# \DocumentsApi

All URIs are relative to *https://localhost/api/v2*

Method | HTTP request | Description
------------- | ------------- | -------------
[**ImmuServiceV2DocumentAudit**](DocumentsApi.md#ImmuServiceV2DocumentAudit) | **Post** /documents/audit | 
[**ImmuServiceV2DocumentInsert**](DocumentsApi.md#ImmuServiceV2DocumentInsert) | **Put** /documents/insert | 
[**ImmuServiceV2DocumentProof**](DocumentsApi.md#ImmuServiceV2DocumentProof) | **Post** /documents/proof | 
[**ImmuServiceV2DocumentSearch**](DocumentsApi.md#ImmuServiceV2DocumentSearch) | **Post** /documents/search | 


# **ImmuServiceV2DocumentAudit**
> Schemav2DocumentAuditResponse ImmuServiceV2DocumentAudit(ctx, body)


### Required Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **ctx** | **context.Context** | context for authentication, logging, cancellation, deadlines, tracing, etc.
  **body** | [**Schemav2DocumentAuditRequest**](Schemav2DocumentAuditRequest.md)|  | 

### Return type

[**Schemav2DocumentAuditResponse**](schemav2DocumentAuditResponse.md)

### Authorization

[bearer](../README.md#bearer)

### HTTP request headers

 - **Content-Type**: application/json
 - **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **ImmuServiceV2DocumentInsert**
> SchemaVerifiableTx ImmuServiceV2DocumentInsert(ctx, body)


### Required Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **ctx** | **context.Context** | context for authentication, logging, cancellation, deadlines, tracing, etc.
  **body** | [**Schemav2DocumentInsertRequest**](Schemav2DocumentInsertRequest.md)|  | 

### Return type

[**SchemaVerifiableTx**](schemaVerifiableTx.md)

### Authorization

[bearer](../README.md#bearer)

### HTTP request headers

 - **Content-Type**: application/json
 - **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **ImmuServiceV2DocumentProof**
> SchemaVerifiableTx ImmuServiceV2DocumentProof(ctx, body)


### Required Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **ctx** | **context.Context** | context for authentication, logging, cancellation, deadlines, tracing, etc.
  **body** | [**Schemav2DocumentProofRequest**](Schemav2DocumentProofRequest.md)|  | 

### Return type

[**SchemaVerifiableTx**](schemaVerifiableTx.md)

### Authorization

[bearer](../README.md#bearer)

### HTTP request headers

 - **Content-Type**: application/json
 - **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **ImmuServiceV2DocumentSearch**
> Schemav2DocumentSearchResponse ImmuServiceV2DocumentSearch(ctx, body)


### Required Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **ctx** | **context.Context** | context for authentication, logging, cancellation, deadlines, tracing, etc.
  **body** | [**Schemav2DocumentSearchRequest**](Schemav2DocumentSearchRequest.md)|  | 

### Return type

[**Schemav2DocumentSearchResponse**](schemav2DocumentSearchResponse.md)

### Authorization

[bearer](../README.md#bearer)

### HTTP request headers

 - **Content-Type**: application/json
 - **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


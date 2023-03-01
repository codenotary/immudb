# \CollectionsApi

All URIs are relative to *https://localhost*

Method | HTTP request | Description
------------- | ------------- | -------------
[**ImmuServiceV2CollectionCreate**](CollectionsApi.md#ImmuServiceV2CollectionCreate) | **Put** /api/collections/create | 
[**ImmuServiceV2CollectionDelete**](CollectionsApi.md#ImmuServiceV2CollectionDelete) | **Delete** /api/collections/delete | 
[**ImmuServiceV2CollectionGet**](CollectionsApi.md#ImmuServiceV2CollectionGet) | **Get** /api/collections/get | 
[**ImmuServiceV2CollectionList**](CollectionsApi.md#ImmuServiceV2CollectionList) | **Post** /api/collections/list | 


# **ImmuServiceV2CollectionCreate**
> SchemaCollectionInformation ImmuServiceV2CollectionCreate(ctx, body)


### Required Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **ctx** | **context.Context** | context for authentication, logging, cancellation, deadlines, tracing, etc.
  **body** | [**SchemaCollectionCreateRequest**](SchemaCollectionCreateRequest.md)|  | 

### Return type

[**SchemaCollectionInformation**](schemaCollectionInformation.md)

### Authorization

[bearer](../README.md#bearer)

### HTTP request headers

 - **Content-Type**: application/json
 - **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **ImmuServiceV2CollectionDelete**
> interface{} ImmuServiceV2CollectionDelete(ctx, optional)


### Required Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **ctx** | **context.Context** | context for authentication, logging, cancellation, deadlines, tracing, etc.
 **optional** | ***CollectionsApiImmuServiceV2CollectionDeleteOpts** | optional parameters | nil if no parameters

### Optional Parameters
Optional parameters are passed through a pointer to a CollectionsApiImmuServiceV2CollectionDeleteOpts struct

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **name** | **optional.String**|  | 

### Return type

**interface{}**

### Authorization

[bearer](../README.md#bearer)

### HTTP request headers

 - **Content-Type**: application/json
 - **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **ImmuServiceV2CollectionGet**
> SchemaCollectionInformation ImmuServiceV2CollectionGet(ctx, optional)


### Required Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **ctx** | **context.Context** | context for authentication, logging, cancellation, deadlines, tracing, etc.
 **optional** | ***CollectionsApiImmuServiceV2CollectionGetOpts** | optional parameters | nil if no parameters

### Optional Parameters
Optional parameters are passed through a pointer to a CollectionsApiImmuServiceV2CollectionGetOpts struct

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **name** | **optional.String**|  | 

### Return type

[**SchemaCollectionInformation**](schemaCollectionInformation.md)

### Authorization

[bearer](../README.md#bearer)

### HTTP request headers

 - **Content-Type**: application/json
 - **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **ImmuServiceV2CollectionList**
> SchemaCollectionListResponse ImmuServiceV2CollectionList(ctx, body)


### Required Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **ctx** | **context.Context** | context for authentication, logging, cancellation, deadlines, tracing, etc.
  **body** | [**SchemaCollectionListRequest**](SchemaCollectionListRequest.md)|  | 

### Return type

[**SchemaCollectionListResponse**](schemaCollectionListResponse.md)

### Authorization

[bearer](../README.md#bearer)

### HTTP request headers

 - **Content-Type**: application/json
 - **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


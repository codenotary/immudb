# \CollectionsApi

All URIs are relative to *https://localhost/api/v2*

Method | HTTP request | Description
------------- | ------------- | -------------
[**ImmuServiceV2CollectionCreate**](CollectionsApi.md#ImmuServiceV2CollectionCreate) | **Put** /collections/create | 
[**ImmuServiceV2CollectionDelete**](CollectionsApi.md#ImmuServiceV2CollectionDelete) | **Delete** /collections/delete | 
[**ImmuServiceV2CollectionGet**](CollectionsApi.md#ImmuServiceV2CollectionGet) | **Get** /collections/get | 
[**ImmuServiceV2CollectionList**](CollectionsApi.md#ImmuServiceV2CollectionList) | **Post** /collections/list | 


# **ImmuServiceV2CollectionCreate**
> Schemav2CollectionInformation ImmuServiceV2CollectionCreate(ctx, body)


### Required Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **ctx** | **context.Context** | context for authentication, logging, cancellation, deadlines, tracing, etc.
  **body** | [**Schemav2CollectionCreateRequest**](Schemav2CollectionCreateRequest.md)|  | 

### Return type

[**Schemav2CollectionInformation**](schemav2CollectionInformation.md)

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
> Schemav2CollectionInformation ImmuServiceV2CollectionGet(ctx, optional)


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

[**Schemav2CollectionInformation**](schemav2CollectionInformation.md)

### Authorization

[bearer](../README.md#bearer)

### HTTP request headers

 - **Content-Type**: application/json
 - **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **ImmuServiceV2CollectionList**
> Schemav2CollectionListResponse ImmuServiceV2CollectionList(ctx, body)


### Required Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **ctx** | **context.Context** | context for authentication, logging, cancellation, deadlines, tracing, etc.
  **body** | [**Schemav2CollectionListRequest**](Schemav2CollectionListRequest.md)|  | 

### Return type

[**Schemav2CollectionListResponse**](schemav2CollectionListResponse.md)

### Authorization

[bearer](../README.md#bearer)

### HTTP request headers

 - **Content-Type**: application/json
 - **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


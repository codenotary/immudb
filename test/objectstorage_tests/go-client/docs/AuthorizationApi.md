# \AuthorizationApi

All URIs are relative to *https://localhost/api/v2*

Method | HTTP request | Description
------------- | ------------- | -------------
[**ImmuServiceV2LoginV2**](AuthorizationApi.md#ImmuServiceV2LoginV2) | **Post** /system/login | 


# **ImmuServiceV2LoginV2**
> Schemav2LoginResponseV2 ImmuServiceV2LoginV2(ctx, body)


### Required Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **ctx** | **context.Context** | context for authentication, logging, cancellation, deadlines, tracing, etc.
  **body** | [**Immudbschemav2LoginRequest**](Immudbschemav2LoginRequest.md)|  | 

### Return type

[**Schemav2LoginResponseV2**](schemav2LoginResponseV2.md)

### Authorization

[bearer](../README.md#bearer)

### HTTP request headers

 - **Content-Type**: application/json
 - **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


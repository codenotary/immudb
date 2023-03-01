# \AuthorizationApi

All URIs are relative to *https://localhost*

Method | HTTP request | Description
------------- | ------------- | -------------
[**ImmuServiceV2LoginV2**](AuthorizationApi.md#ImmuServiceV2LoginV2) | **Post** /api/system/login | 


# **ImmuServiceV2LoginV2**
> SchemaLoginResponseV2 ImmuServiceV2LoginV2(ctx, body)


### Required Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **ctx** | **context.Context** | context for authentication, logging, cancellation, deadlines, tracing, etc.
  **body** | [**SchemaLoginRequest**](SchemaLoginRequest.md)|  | 

### Return type

[**SchemaLoginResponseV2**](schemaLoginResponseV2.md)

### Authorization

[bearer](../README.md#bearer)

### HTTP request headers

 - **Content-Type**: application/json
 - **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


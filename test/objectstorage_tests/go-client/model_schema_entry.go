/*
 * immudb REST API v2
 *
 * No description provided (generated by Swagger Codegen https://github.com/swagger-api/swagger-codegen)
 *
 * API version: version not set
 * Generated by: Swagger Codegen (https://github.com/swagger-api/swagger-codegen.git)
 */

package swagger

type SchemaEntry struct {
	Tx string `json:"tx,omitempty"`
	Key string `json:"key,omitempty"`
	Value string `json:"value,omitempty"`
	ReferencedBy *SchemaReference `json:"referencedBy,omitempty"`
	Metadata *SchemaKvMetadata `json:"metadata,omitempty"`
	Expired bool `json:"expired,omitempty"`
	Revision string `json:"revision,omitempty"`
}

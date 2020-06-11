/*
Copyright 2019-2020 vChain, Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

	http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package server

// MTLsOptions ...
type MTLsOptions struct {
	Pkey        string
	Certificate string
	ClientCAs   string
}

// DefaultMTLsOptions ...
func DefaultMTLsOptions() MTLsOptions {
	return MTLsOptions{
		Pkey:        "./tools/mtls/3_application/private/localhost.key.pem",
		Certificate: "./tools/mtls/3_application/certs/localhost.cert.pem",
		ClientCAs:   "./tools/mtls/2_intermediate/certs/ca-chain.cert.pem",
	}
}

// WithPkey ...
func (o MTLsOptions) WithPkey(Pkey string) MTLsOptions {
	o.Pkey = Pkey
	return o
}

// WithCertificate ...
func (o MTLsOptions) WithCertificate(Certificate string) MTLsOptions {
	o.Certificate = Certificate
	return o
}

// WithClientCAs ...
func (o MTLsOptions) WithClientCAs(ClientCAs string) MTLsOptions {
	o.ClientCAs = ClientCAs
	return o
}

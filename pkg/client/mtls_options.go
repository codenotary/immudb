/*
Copyright 2024 Codenotary Inc. All rights reserved.

SPDX-License-Identifier: BUSL-1.1
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    https://mariadb.com/bsl11/

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package client

// MTLsOptions mTLS options
type MTLsOptions struct {
	Servername  string
	Pkey        string
	Certificate string
	ClientCAs   string
}

// DefaultMTLsOptions returns the default mTLS options
func DefaultMTLsOptions() MTLsOptions {
	return MTLsOptions{
		Servername:  "localhost",
		Pkey:        "./tools/mtls/4_client/private/localhost.key.pem",
		Certificate: "./tools/mtls/4_client/certs/localhost.cert.pem",
		ClientCAs:   "./tools/mtls/2_intermediate/certs/ca-chain.cert.pem",
	}
}

// WithServername sets the server name
func (o MTLsOptions) WithServername(servername string) MTLsOptions {
	o.Servername = servername
	return o
}

// WithPkey sets the client private key
func (o MTLsOptions) WithPkey(pkey string) MTLsOptions {
	o.Pkey = pkey
	return o
}

// WithCertificate sets the client certificate
func (o MTLsOptions) WithCertificate(certificate string) MTLsOptions {
	o.Certificate = certificate
	return o
}

// WithClientCAs sets a list of CA certificates
func (o MTLsOptions) WithClientCAs(clientCAs string) MTLsOptions {
	o.ClientCAs = clientCAs
	return o
}

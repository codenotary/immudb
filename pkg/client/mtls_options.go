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

package client

import (
	"os"
)

type MTLsOptions struct {
	Pkey        string
	Certificate string
	ClientCAs   string
}

func DefaultMTLsOptions() MTLsOptions {
	return MTLsOptions{
		Pkey:        "./tools/mtls/4_client/private/localhost.key.pem",
		Certificate: "./tools/mtls/4_client/certs/localhost.cert.pem",
		ClientCAs:   "./tools/mtls/2_intermediate/certs/ca-chain.cert.pem",
	}
}

func (o MTLsOptions) WithPkey(Pkey string) MTLsOptions {
	o.Pkey = Pkey
	return o
}

func (o MTLsOptions) WithCertificate(Certificate string) MTLsOptions {
	o.Certificate = Certificate
	return o
}

func (o MTLsOptions) WithClientCAs(ClientCAs string) MTLsOptions {
	o.ClientCAs = ClientCAs
	return o
}

func (o MTLsOptions) FromEnvironment() MTLsOptions {
	Pkey := os.Getenv("IMMUD_PKEY")
	if Pkey != "" {
		o.Pkey = Pkey
	}
	Certificate := os.Getenv("IMMUD_CERTIFICATE")
	if Certificate != "" {
		o.Certificate = Certificate
	}
	ClientCAs := os.Getenv("IMMUD_CLIENT_CAS")
	if ClientCAs != "" {
		o.ClientCAs = ClientCAs
	}
	return o
}

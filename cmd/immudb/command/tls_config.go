/*
Copyright 2022 CodeNotary, Inc. All rights reserved.

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

package immudb

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io/ioutil"
)

func setUpTLS(pkey, cert, ca string, mtls bool) (*tls.Config, error) {
	var c *tls.Config

	if cert != "" && pkey != "" {
		certs, err := tls.LoadX509KeyPair(cert, pkey)
		if err != nil {
			return nil, errors.New(fmt.Sprintf("failed to read client certificate or private key: %v", err))
		}
		c = &tls.Config{
			Certificates: []tls.Certificate{certs},
			ClientAuth:   tls.VerifyClientCertIfGiven,
		}
	}

	if mtls && (cert == "" || pkey == "") {
		return nil, errors.New("in order to enable MTLS a certificate and private key are required")
	}

	// if CA is not provided there is an automatic load of local CA in os
	if mtls && ca != "" {
		certPool := x509.NewCertPool()
		// Trusted store, contain the list of trusted certificates. client has to use one of this certificate to be trusted by this server
		bs, err := ioutil.ReadFile(ca)
		if err != nil {
			return nil, fmt.Errorf("failed to read client ca cert: %v", err)
		}

		ok := certPool.AppendCertsFromPEM(bs)
		if !ok {
			return nil, fmt.Errorf("failed to append client certs: %v", err)
		}
		c.ClientCAs = certPool
	}

	return c, nil
}

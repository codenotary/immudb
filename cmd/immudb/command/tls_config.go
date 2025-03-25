/*
Copyright 2025 Codenotary Inc. All rights reserved.

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

package immudb

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	tlscert "github.com/codenotary/immudb/pkg/cert"
)

const (
	certFileDefault = "immudb-cert.pem"
	keyFileDefault  = "immudb-key.pem"

	certOrganizationDefault = "immudb"
	certExpirationDefault   = 365 * 24 * time.Hour
)

func setUpTLS(pkeyPath, certPath, ca string, mtls bool, autoCert bool) (*tls.Config, error) {
	if (pkeyPath == "" && certPath != "") || (pkeyPath != "" && certPath == "") {
		return nil, fmt.Errorf("both certificate and private key paths must be specified or neither")
	}

	var c *tls.Config

	certPath, pkeyPath, err := getCertAndKeyPath(certPath, pkeyPath, autoCert)
	if err != nil {
		return nil, err
	}

	if certPath != "" && pkeyPath != "" {
		cert, err := ensureCert(certPath, pkeyPath, autoCert)
		if err != nil {
			return nil, fmt.Errorf("failed to read client certificate or private key: %v", err)
		}

		c = &tls.Config{
			Certificates: []tls.Certificate{*cert},
			ClientAuth:   tls.VerifyClientCertIfGiven,
		}

		if autoCert {
			rootCert, err := os.ReadFile(certPath)
			if err != nil {
				return nil, fmt.Errorf("failed to read root cert: %v", err)
			}

			rootPool := x509.NewCertPool()
			if ok := rootPool.AppendCertsFromPEM(rootCert); !ok {
				return nil, fmt.Errorf("failed to read root cert")
			}
			c.RootCAs = rootPool
		}
	}

	if mtls && (certPath == "" || pkeyPath == "") {
		return nil, errors.New("in order to enable MTLS a certificate and private key are required")
	}

	// if CA is not provided there is an automatic load of local CA in os
	if mtls && ca != "" {
		certPool := x509.NewCertPool()
		// Trusted store, contain the list of trusted certificates. client has to use one of this certificate to be trusted by this server
		bs, err := os.ReadFile(ca)
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

func loadCert(certPath, keyPath string) (*tls.Certificate, error) {
	cert, err := tls.LoadX509KeyPair(certPath, keyPath)
	if err != nil {
		return nil, fmt.Errorf("failed to load cert/key pair: %w", err)
	}
	return &cert, nil
}

func ensureCert(certPath, keyPath string, genCert bool) (*tls.Certificate, error) {
	_, err1 := os.Stat(certPath)
	_, err2 := os.Stat(keyPath)

	if (os.IsNotExist(err1) || os.IsNotExist(err2)) && genCert {
		if err := tlscert.GenerateSelfSignedCert(certPath, keyPath, certOrganizationDefault, certExpirationDefault); err != nil {
			return nil, err
		}
	}
	return loadCert(certPath, keyPath)
}

func getCertAndKeyPath(certPath, keyPath string, useDefault bool) (string, string, error) {
	if !useDefault || (certPath != "" && keyPath != "") {
		return certPath, keyPath, nil
	}

	homeDir, err := os.UserHomeDir()
	if err != nil {
		return "", "", fmt.Errorf("cannot get user home directory: %w", err)
	}

	return filepath.Join(homeDir, "immudb", "ssl", certFileDefault),
		filepath.Join(homeDir, "immudb", "ssl", keyFileDefault),
		nil
}

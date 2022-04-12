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

package server

import (
	"crypto/tls"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"net/http"
	"testing"
	"time"
)

func TestStartWebServerHTTP(t *testing.T) {
	options := DefaultOptions()
	server := DefaultServer().WithOptions(options).(*ImmuServer)

	tlsConfig := &tls.Config{
		Certificates: []tls.Certificate{},
		ClientAuth:   tls.VerifyClientCertIfGiven,
	}

	webServer, err := StartWebServer(
		"0.0.0.0:8080",
		tlsConfig,
		server,
		&mockLogger{})
	require.NoError(t, err)
	defer webServer.Close()

	assert.IsType(t, &http.Server{}, webServer)

	client := &http.Client{}
	assert.Eventually(t, func() bool {
		_, err = client.Get("http://0.0.0.0:8080")
		return err == nil
	}, 1*time.Second, 30*time.Millisecond)
}

func TestStartWebServerHTTPS(t *testing.T) {
	options := DefaultOptions()
	server := DefaultServer().WithOptions(options).(*ImmuServer)
	certPem := []byte(`-----BEGIN CERTIFICATE-----
MIIBhTCCASugAwIBAgIQIRi6zePL6mKjOipn+dNuaTAKBggqhkjOPQQDAjASMRAw
DgYDVQQKEwdBY21lIENvMB4XDTE3MTAyMDE5NDMwNloXDTE4MTAyMDE5NDMwNlow
EjEQMA4GA1UEChMHQWNtZSBDbzBZMBMGByqGSM49AgEGCCqGSM49AwEHA0IABD0d
7VNhbWvZLWPuj/RtHFjvtJBEwOkhbN/BnnE8rnZR8+sbwnc/KhCk3FhnpHZnQz7B
5aETbbIgmuvewdjvSBSjYzBhMA4GA1UdDwEB/wQEAwICpDATBgNVHSUEDDAKBggr
BgEFBQcDATAPBgNVHRMBAf8EBTADAQH/MCkGA1UdEQQiMCCCDmxvY2FsaG9zdDo1
NDUzgg4xMjcuMC4wLjE6NTQ1MzAKBggqhkjOPQQDAgNIADBFAiEA2zpJEPQyz6/l
Wf86aX6PepsntZv2GYlA5UpabfT2EZICICpJ5h/iI+i341gBmLiAFQOyTDT+/wQc
6MF9+Yw1Yy0t
-----END CERTIFICATE-----`)
	keyPem := []byte(`-----BEGIN EC PRIVATE KEY-----
MHcCAQEEIIrYSSNQFaA2Hwf1duRSxKtLYX5CB04fSeQ6tF1aY/PuoAoGCCqGSM49
AwEHoUQDQgAEPR3tU2Fta9ktY+6P9G0cWO+0kETA6SFs38GecTyudlHz6xvCdz8q
EKTcWGekdmdDPsHloRNtsiCa697B2O9IFA==
-----END EC PRIVATE KEY-----`)

	cert, err := tls.X509KeyPair(certPem, keyPem)
	require.NoError(t, err)
	tlsConfig := &tls.Config{Certificates: []tls.Certificate{cert}}

	webServer, err := StartWebServer(
		"0.0.0.0:8080",
		tlsConfig,
		server,
		&mockLogger{})
	require.NoError(t, err)
	defer webServer.Close()

	assert.IsType(t, &http.Server{}, webServer)

	tr := &http.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: true}}
	client := &http.Client{Transport: tr}
	assert.Eventually(t, func() bool {
		_, err = client.Get("https://0.0.0.0:8080")
		return err == nil
	}, 1*time.Second, 30*time.Millisecond)
}

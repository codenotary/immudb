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

package server

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"math/big"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func freePort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	port := l.Addr().(*net.TCPAddr).Port
	l.Close()
	return port
}

func TestStartWebServerHTTP(t *testing.T) {
	dir := t.TempDir()

	options := DefaultOptions().
		WithDir(dir).
		WithAddress("127.0.0.1").
		WithPort(freePort(t)).
		WithWebServerPort(freePort(t))
	server := DefaultServer().WithOptions(options).(*ImmuServer)

	tlsConfig := &tls.Config{
		Certificates: []tls.Certificate{},
		ClientAuth:   tls.VerifyClientCertIfGiven,
	}

	webServer, err := startWebServer(
		context.Background(),
		options.Bind(),
		options.WebBind(),
		tlsConfig,
		server,
		&mockLogger{})
	require.NoError(t, err)
	defer webServer.Close()

	require.IsType(t, &http.Server{}, webServer)

	client := &http.Client{}
	require.Eventually(t, func() bool {
		_, err = client.Get("http://" + options.WebBind())
		return err == nil
	}, 10*time.Second, 30*time.Millisecond)
}

func TestStartWebServerHTTPS(t *testing.T) {
	dir := t.TempDir()

	options := DefaultOptions().
		WithDir(dir).
		WithAddress("127.0.0.1").
		WithPort(freePort(t)).
		WithWebServerPort(freePort(t))
	server := DefaultServer().WithOptions(options).(*ImmuServer)

	tlsConfig := tlsConfigTest(t)
	webServer, err := startWebServer(
		context.Background(),
		options.Bind(),
		options.WebBind(),
		tlsConfig,
		server,
		&mockLogger{})
	require.NoError(t, err)
	defer webServer.Close()

	require.IsType(t, &http.Server{}, webServer)

	tr := &http.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: true}}
	client := &http.Client{Transport: tr}
	require.Eventually(t, func() bool {
		_, err = client.Get("https://" + options.WebBind())
		return err == nil
	}, 10*time.Second, 30*time.Millisecond)
}

func tlsConfigTest(t *testing.T) *tls.Config {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)

	template := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		NotBefore:    time.Now(),
		NotAfter:     time.Now().Add(time.Hour),
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
		DNSNames:     []string{"localhost"},
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}

	certDER, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	require.NoError(t, err)

	return &tls.Config{
		Certificates: []tls.Certificate{{
			Certificate: [][]byte{certDER},
			PrivateKey:  key,
		}},
	}
}

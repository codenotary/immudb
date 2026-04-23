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

package cert

import (
	"crypto/x509"
	"encoding/pem"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestGenerateSelfSignedCert(t *testing.T) {
	dir := t.TempDir()
	certPath := filepath.Join(dir, "cert.pem")
	keyPath := filepath.Join(dir, "key.pem")

	err := GenerateSelfSignedCert(certPath, keyPath, "TestOrg", 24*time.Hour)
	require.NoError(t, err)

	// Verify cert file
	certData, err := os.ReadFile(certPath)
	require.NoError(t, err)

	block, _ := pem.Decode(certData)
	require.NotNil(t, block)
	require.Equal(t, "CERTIFICATE", block.Type)

	cert, err := x509.ParseCertificate(block.Bytes)
	require.NoError(t, err)

	require.Equal(t, []string{"TestOrg"}, cert.Subject.Organization)
	require.Contains(t, cert.DNSNames, "localhost")

	hasZeroIP := false
	for _, ip := range cert.IPAddresses {
		if ip.Equal(net.ParseIP("0.0.0.0")) {
			hasZeroIP = true
		}
	}
	require.True(t, hasZeroIP, "cert should contain 0.0.0.0 IP")

	require.WithinDuration(t, time.Now().Add(24*time.Hour), cert.NotAfter, time.Minute)

	require.Equal(t, x509.KeyUsageKeyEncipherment|x509.KeyUsageDigitalSignature, cert.KeyUsage)

	// Verify key file
	keyData, err := os.ReadFile(keyPath)
	require.NoError(t, err)

	keyBlock, _ := pem.Decode(keyData)
	require.NotNil(t, keyBlock)
	require.Equal(t, "PRIVATE KEY", keyBlock.Type)

	key, err := x509.ParsePKCS1PrivateKey(keyBlock.Bytes)
	require.NoError(t, err)
	require.Equal(t, 2048, key.N.BitLen())
}

func TestGenerateSelfSignedCertCreatesDir(t *testing.T) {
	dir := t.TempDir()
	certPath := filepath.Join(dir, "sub", "dir", "cert.pem")
	keyPath := filepath.Join(dir, "sub", "dir", "key.pem")

	err := GenerateSelfSignedCert(certPath, keyPath, "TestOrg", time.Hour)
	require.NoError(t, err)

	_, err = os.Stat(certPath)
	require.NoError(t, err)
	_, err = os.Stat(keyPath)
	require.NoError(t, err)
}

func TestGenerateSelfSignedCertInvalidPath(t *testing.T) {
	err := GenerateSelfSignedCert("/dev/null/impossible/cert.pem", "/dev/null/impossible/key.pem", "TestOrg", time.Hour)
	require.Error(t, err)
}

func TestEncodePEM(t *testing.T) {
	data := []byte("test data")
	result := encodePEM(data, "TEST BLOCK")

	block, rest := pem.Decode(result)
	require.NotNil(t, block)
	require.Empty(t, rest)
	require.Equal(t, "TEST BLOCK", block.Type)
	require.Equal(t, data, block.Bytes)
}

func TestListIPs(t *testing.T) {
	ips, err := listIPs()
	require.NoError(t, err)
	require.NotEmpty(t, ips, "should return at least one IP")
}

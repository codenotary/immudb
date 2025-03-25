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
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math/big"
	"net"
	"os"
	"path"
	"time"
)

func GenerateSelfSignedCert(certPath, keyPath string, org string, expiration time.Duration) error {
	priv, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return fmt.Errorf("failed to generate RSA key: %w", err)
	}

	notBefore := time.Now()
	notAfter := notBefore.Add(expiration)

	serialNumber, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		return fmt.Errorf("failed to generate serial number: %w", err)
	}

	hostname, err := os.Hostname()
	if err != nil {
		return err
	}

	ips, err := listIPs()
	if err != nil {
		return err
	}
	ips = append(ips, net.ParseIP("0.0.0.0"))

	issuerOrSubject := pkix.Name{
		Organization: []string{org},
	}

	template := x509.Certificate{
		Issuer:       issuerOrSubject,
		SerialNumber: serialNumber,
		Subject:      issuerOrSubject,
		DNSNames:     []string{"localhost", hostname},
		NotBefore:    notBefore,
		NotAfter:     notAfter,
		IPAddresses:  ips,
		KeyUsage:     x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}

	certBytes, err := x509.CreateCertificate(rand.Reader, &template, &template, &priv.PublicKey, priv)
	if err != nil {
		return fmt.Errorf("failed to create certificate: %w", err)
	}

	if err := os.MkdirAll(path.Dir(certPath), 0755); err != nil {
		return err
	}

	certBytesPem := encodePEM(certBytes, "CERTIFICATE")
	if err := os.WriteFile(certPath, certBytesPem, 0644); err != nil {
		return fmt.Errorf("failed to write cert file: %w", err)
	}

	privBytes := x509.MarshalPKCS1PrivateKey(priv)
	privBytesPem := encodePEM(privBytes, "PRIVATE KEY")

	if err := os.WriteFile(keyPath, privBytesPem, 0600); err != nil {
		return fmt.Errorf("failed to write key file: %w", err)
	}
	return nil
}

func encodePEM(data []byte, blockType string) []byte {
	block := &pem.Block{
		Type:  blockType,
		Bytes: data,
	}
	return pem.EncodeToMemory(block)
}

func listIPs() ([]net.IP, error) {
	addresses, err := net.InterfaceAddrs()
	if err != nil {
		return nil, err
	}

	ips := make([]net.IP, 0, len(addresses))
	for _, addr := range addresses {
		ipNet, ok := addr.(*net.IPNet)
		if ok {
			ips = append(ips, ipNet.IP)
		}
	}
	return ips, nil
}

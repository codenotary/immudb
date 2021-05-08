/*
Copyright 2021 CodeNotary, Inc. All rights reserved.

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
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestStartWebServerHTTP(t *testing.T) {
	options := DefaultOptions()
	server := DefaultServer().WithOptions(options).(*ImmuServer)

	webServer, err := StartWebServer(
		"0.0.0.0:8080",
		nil,
		server,
		&mockLogger{})
	defer webServer.Close()

	assert.IsType(t, &http.Server{}, webServer)
	assert.Nil(t, webServer.TLSConfig)
	assert.Nil(t, err)
}

func TestStartWebServerHTTPS(t *testing.T) {
	options := DefaultOptions()
	tlsConfig := &tls.Config{
		Certificates: []tls.Certificate{},
		ClientAuth: tls.VerifyClientCertIfGiven,
	}

	server := DefaultServer().WithOptions(options).(*ImmuServer)

	webServer, err := StartWebServer(
		"0.0.0.0:8080",
		tlsConfig,
		server,
		&mockLogger{})
	defer webServer.Close()

	assert.IsType(t, &http.Server{}, webServer)
	assert.NotNil(t, webServer.TLSConfig)
	assert.Nil(t, err)
}

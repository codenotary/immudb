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

package cache

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"github.com/rogpeppe/go-internal/lockedfile"
)

const (
	IDENTITY_FN       = ".identity-"
	identityHashBytes = 16
)

func getFilenameForServerIdentity(serverIdentity, identityDir string) string {
	identityHashRaw := sha256.Sum256([]byte(serverIdentity))
	identityHash := base64.RawURLEncoding.EncodeToString(identityHashRaw[:identityHashBytes])
	return filepath.Join(identityDir, IDENTITY_FN+identityHash)
}

func validateServerIdentityInFile(serverIdentity string, serverUUID string, identityDir string) error {

	identityFile := getFilenameForServerIdentity(serverIdentity, identityDir)

	fl, err := lockedfile.OpenFile(identityFile, os.O_RDWR|os.O_CREATE, 0655)
	if err != nil {
		return fmt.Errorf("could not check the identity of the server '%s' in file '%s': %w", serverIdentity, identityFile, err)
	}
	defer fl.Close()

	identityDataJson := struct {
		ServerUUID     string `json:"serverUUID"`
		ServerIdentity string `json:"serverIdentity"`
	}{}

	stat, err := fl.Stat()
	if err != nil {
		return fmt.Errorf("could not check the identity of the server '%s' in file '%s': %w", serverIdentity, identityFile, err)
	}

	if stat.Size() == 0 {
		// File is empty - which may mean that it was just created,
		// write the new identity data
		identityDataJson.ServerUUID = serverUUID
		identityDataJson.ServerIdentity = serverIdentity

		enc := json.NewEncoder(fl)
		enc.SetIndent("", "\t")

		err := enc.Encode(&identityDataJson)
		if err != nil {
			return fmt.Errorf("could not store the identity of the server '%s' in file '%s': %w", serverIdentity, identityFile, err)
		}

		err = fl.Close()
		if err != nil {
			return fmt.Errorf("could not store the identity of the server '%s' in file '%s': %w", serverIdentity, identityFile, err)
		}

		return nil
	}

	// File exists and is not empty, check if the server UUID matches
	// what is already stored in the file.
	err = json.NewDecoder(fl).Decode(&identityDataJson)
	if err != nil {
		return fmt.Errorf(
			"could not check the identity of the server '%s' in file '%s': %w, "+
				"if you still want to connect to a different immudb server instance with the same identity, "+
				"please remove the identity file",
			serverIdentity,
			identityFile,
			err,
		)
	}

	if identityDataJson.ServerUUID != serverUUID {
		return fmt.Errorf(
			"could not check the identity of the server '%s' in file '%s': %w, "+
				"if you still want to connect to a different immudb server instance with the same identity, "+
				"please remove the identity file",
			serverIdentity,
			identityFile,
			ErrServerIdentityValidationFailed,
		)
	}

	return nil
}

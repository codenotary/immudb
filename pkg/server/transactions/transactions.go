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

package transactions

import (
	"github.com/codenotary/immudb/pkg/logger"
	"sync"
)

type transaction struct {
	sync.Mutex
	transactionID     string
	log               logger.Logger
	readWrite         bool
	onDeleteCallbacks map[string]func() error
	callbacksWG       *sync.WaitGroup
}

type Transaction interface {
	GetID() string
	AddOnDeleteCallback(name string, f func() error)
	Delete()
}

func NewTransaction(transactionID string, readWrite bool, log logger.Logger, callbacksWG *sync.WaitGroup) *transaction {
	return &transaction{
		transactionID:     transactionID,
		log:               log,
		readWrite:         readWrite,
		onDeleteCallbacks: make(map[string]func() error),
		callbacksWG:       callbacksWG,
	}
}

func (tx *transaction) GetID() string {
	return tx.transactionID
}

func (tx *transaction) AddOnDeleteCallback(name string, f func() error) {
	tx.Lock()
	defer tx.Unlock()
	tx.onDeleteCallbacks[name] = f
}

func (tx *transaction) Delete() {
	tx.Lock()
	defer tx.Unlock()
	for cName, c := range tx.onDeleteCallbacks {
		go func(callbackName string, callback func() error) {
			tx.callbacksWG.Add(1)
			defer tx.callbacksWG.Done()
			if err := callback(); err != nil {
				tx.log.Errorf("error on callback %s on transaction ID %s: %v", callbackName, tx.transactionID, err)
			}
		}(cName, c)
	}
}

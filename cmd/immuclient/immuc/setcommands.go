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

package immuc

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"strconv"
	"time"

	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/codenotary/immudb/pkg/client"
)

func (i *immuc) Set(args []string) (string, error) {
	var reader io.Reader

	if len(args) > 1 {
		reader = bytes.NewReader([]byte(args[1]))
	} else {
		reader = bufio.NewReader(os.Stdin)
	}

	var buf bytes.Buffer
	tee := io.TeeReader(reader, &buf)

	key, err := ioutil.ReadAll(bytes.NewReader([]byte(args[0])))
	if err != nil {
		return "", err
	}

	value, err := ioutil.ReadAll(tee)
	if err != nil {
		return "", err
	}

	ctx := context.Background()
	response, err := i.Execute(func(immuClient client.ImmuClient) (interface{}, error) {
		return immuClient.Set(ctx, key, value)
	})
	if err != nil {
		return "", err
	}

	time.Sleep(1 * time.Millisecond)

	value2, err := ioutil.ReadAll(&buf)
	if err != nil {
		return "", err
	}

	txMeta := response.(*schema.TxMetadata)
	scstr, err := i.Execute(func(immuClient client.ImmuClient) (interface{}, error) {
		return immuClient.GetSince(ctx, key, txMeta.Id)
	})
	if err != nil {
		return "", err
	}

	return PrintKV([]byte(args[0]), value2, scstr.(*schema.Entry).Tx, false, false), nil
}

func (i *immuc) VerifiedSet(args []string) (string, error) {
	var reader io.Reader

	if len(args) > 1 {
		reader = bytes.NewReader([]byte(args[1]))
	} else {
		reader = bufio.NewReader(os.Stdin)
	}

	key, err := ioutil.ReadAll(bytes.NewReader([]byte(args[0])))
	if err != nil {
		return "", err
	}

	var buf bytes.Buffer
	tee := io.TeeReader(reader, &buf)

	value, err := ioutil.ReadAll(tee)
	if err != nil {
		return "", err
	}

	ctx := context.Background()
	if _, err = i.Execute(func(immuClient client.ImmuClient) (interface{}, error) {
		return immuClient.VerifiedSet(ctx, key, value)
	}); err != nil {
		return "", err
	}

	time.Sleep(1 * time.Millisecond)

	value2, err := ioutil.ReadAll(&buf)
	if err != nil {
		return "", err
	}

	vi, err := i.Execute(func(immuClient client.ImmuClient) (interface{}, error) {
		return immuClient.VerifiedGet(ctx, key)
	})
	if err != nil {
		return "", err
	}

	return PrintKV([]byte(args[0]), value2, vi.(*schema.Entry).Tx, true, false), nil
}

func (i *immuc) ZAdd(args []string) (string, error) {
	var setReader io.Reader
	var scoreReader io.Reader
	var keyReader io.Reader

	if len(args) > 1 {
		setReader = bytes.NewReader([]byte(args[0]))
		scoreReader = bytes.NewReader([]byte(args[1]))
		keyReader = bytes.NewReader([]byte(args[2]))
	}

	bs, err := ioutil.ReadAll(scoreReader)
	if err != nil {
		return "", err
	}

	score, err := strconv.ParseFloat(string(bs), 64)
	if err != nil {
		return "", err
	}

	set, err := ioutil.ReadAll(setReader)
	if err != nil {
		return "", err
	}

	key, err := ioutil.ReadAll(keyReader)
	if err != nil {
		return "", err
	}

	ctx := context.Background()
	txMeta, err := i.Execute(func(immuClient client.ImmuClient) (interface{}, error) {
		return immuClient.ZAdd(ctx, set, score, key)
	})
	if err != nil {
		return "", err
	}

	return PrintSetItem(set, key, score, txMeta.(*schema.TxMetadata), false), nil
}

func (i *immuc) VerifiedZAdd(args []string) (string, error) {
	var setReader io.Reader
	var scoreReader io.Reader
	var keyReader io.Reader

	if len(args) > 1 {
		setReader = bytes.NewReader([]byte(args[0]))
		scoreReader = bytes.NewReader([]byte(args[1]))
		keyReader = bytes.NewReader([]byte(args[2]))
	}

	bs, err := ioutil.ReadAll(scoreReader)
	if err != nil {
		return "", err
	}

	score, err := strconv.ParseFloat(string(bs), 64)
	if err != nil {
		return "", err
	}

	set, err := ioutil.ReadAll(setReader)
	if err != nil {
		return "", err
	}

	key, err := ioutil.ReadAll(keyReader)
	if err != nil {
		return "", err
	}

	ctx := context.Background()
	response, err := i.Execute(func(immuClient client.ImmuClient) (interface{}, error) {
		return immuClient.VerifiedZAdd(ctx, set, score, key)
	})
	if err != nil {
		return "", err
	}

	resp := PrintSetItem([]byte(args[0]), []byte(args[2]), score, response.(*schema.TxMetadata), true)

	return resp, nil
}

func (i *immuc) CreateDatabase(args []string) (string, error) {
	if len(args) < 1 {
		return "", fmt.Errorf("ERROR: Not enough arguments. Use [command] --help for documentation ")
	}

	dbname := args[0]
	ctx := context.Background()
	if _, err := i.Execute(func(immuClient client.ImmuClient) (interface{}, error) {
		return nil, immuClient.CreateDatabase(ctx, &schema.DatabaseSettings{
			DatabaseName: string(dbname),
		})
	}); err != nil {
		return "", err
	}

	return "database successfully created", nil
}

func (i *immuc) DatabaseList(args []string) (string, error) {
	resp, err := i.Execute(func(immuClient client.ImmuClient) (interface{}, error) {
		return immuClient.DatabaseList(context.Background())
	})
	if err != nil {
		return "", err
	}

	var dbList string

	for _, val := range resp.(*schema.DatabaseListResponse).Databases {
		if i.options.CurrentDatabase == val.DatabaseName {
			dbList += "*"
		}
		dbList += fmt.Sprintf("%s", val.DatabaseName)
	}

	return dbList, nil
}

func (i *immuc) UseDatabase(args []string) (string, error) {
	var dbname string
	if len(args) > 0 {
		dbname = args[0]
	} else if len(i.options.Database) > 0 {
		dbname = i.options.Database
	} else {
		return "", fmt.Errorf("database name not specified")
	}

	ctx := context.Background()
	resp, err := i.Execute(func(immuClient client.ImmuClient) (interface{}, error) {
		return immuClient.UseDatabase(ctx, &schema.Database{
			DatabaseName: dbname,
		})
	})
	if err != nil {
		return "", err
	}

	i.ImmuClient.GetOptions().CurrentDatabase = dbname

	if err = i.ts.SetToken(dbname, resp.(*schema.UseDatabaseReply).Token); err != nil {
		return "", err
	}

	i.ImmuClient, err = client.NewImmuClient((i.ImmuClient.GetOptions()))
	if err != nil {
		return "", err
	}

	return fmt.Sprintf("Now using %s", dbname), nil
}

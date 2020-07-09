/*
Copyright 2019-2020 vChain, Inc.

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

	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/codenotary/immudb/pkg/client"
	"google.golang.org/protobuf/types/known/emptypb"
)

func (i *immuc) RawSafeSet(args []string) (string, error) {
	key, err := ioutil.ReadAll(bytes.NewReader([]byte(args[0])))
	if err != nil {
		return "", err
	}
	val, err := ioutil.ReadAll(bytes.NewReader([]byte(args[0])))
	if err != nil {
		return "", err
	}

	ctx := context.Background()
	_, err = i.ImmuClient.RawSafeSet(ctx, key, val)
	if err != nil {
		return "", err
	}
	vi, err := i.ImmuClient.RawSafeGet(ctx, key)

	if err != nil {
		return "", err
	}
	return PrintItem(vi.Key, vi.Value, vi, false), nil
}

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
	_, err = i.ImmuClient.Set(ctx, key, value)
	if err != nil {
		return "", err
	}
	value2, err := ioutil.ReadAll(&buf)
	if err != nil {
		return "", err
	}
	scstr, err := i.ImmuClient.Get(ctx, key)
	if err != nil {
		return "", err
	}

	return PrintItem([]byte(args[0]), value2, scstr, false), nil
}

func (i *immuc) SafeSet(args []string) (string, error) {
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
	_, err = i.ImmuClient.SafeSet(ctx, key, value)
	if err != nil {
		return "", err
	}
	value2, err := ioutil.ReadAll(&buf)
	if err != nil {
		return "", err
	}
	vi, err := i.ImmuClient.SafeGet(ctx, key)
	if err != nil {
		return "", err
	}

	return PrintItem([]byte(args[0]), value2, vi, false), nil
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
	response, err := i.ImmuClient.ZAdd(ctx, set, score, key)
	if err != nil {
		return "", err
	}

	return PrintSetItem([]byte(args[0]), []byte(args[2]), score, response), nil
}

func (i *immuc) SafeZAdd(args []string) (string, error) {
	var setReader io.Reader
	var scoreReader io.Reader
	var keyReader io.Reader
	if len(args) > 1 {
		setReader = bytes.NewReader([]byte(args[0]))
		scoreReader = bytes.NewReader([]byte(args[1]))
		keyReader = bytes.NewReader([]byte(args[2]))
	}
	bs, err := ioutil.ReadAll(scoreReader)
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
	response, err := i.ImmuClient.SafeZAdd(ctx, set, score, key)
	if err != nil {
		return "", err
	}
	resp := PrintSetItem([]byte(args[0]), []byte(args[2]), score, response)
	return resp, nil
}

func (i *immuc) CreateDatabase(args []string) (string, error) {
	if len(args) < 1 {
		return "", fmt.Errorf("ERROR: Not enough arguments. Use [command] --help for documentation ")
	}
	dbname := args[0]
	ctx := context.Background()
	resp, err := i.ImmuClient.CreateDatabase(ctx, &schema.Database{
		Databasename: string(dbname),
	})
	if err != nil {
		return "", err
	}
	return resp.Error.Errormessage, nil
}
func (i *immuc) DatabaseList(args []string) (string, error) {
	resp, err := i.ImmuClient.DatabaseList(context.Background(), &emptypb.Empty{})
	if err != nil {
		return "", err
	}

	for _, val := range resp.Databases {
		if i.options.CurrentDatabase == val.Databasename {
			fmt.Print("*")
		}
		fmt.Println(val.Databasename)
	}
	return "", nil

}
func (i *immuc) UseDatabase(args []string) (string, error) {
	dbname := args[0]

	ctx := context.Background()
	resp, err := i.ImmuClient.UseDatabase(ctx, &schema.Database{
		Databasename: dbname,
	})
	if err != nil {
		return "", err
	}
	if resp.Error.Errorcode == schema.ErrorCodes_Ok {
		i.ImmuClient.GetOptions().CurrentDatabase = dbname
		tokenFileName := i.ImmuClient.GetOptions().TokenFileName
		if err = i.hds.WriteFileToUserHomeDir([]byte(resp.Token), tokenFileName); err != nil {
			return "", err
		}
		i.ImmuClient, err = client.NewImmuClient((i.ImmuClient.GetOptions()))
		if err != nil {
			return "", err
		}
	}

	return resp.Error.Errormessage, nil
}

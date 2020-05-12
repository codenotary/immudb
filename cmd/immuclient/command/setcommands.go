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

package immuclient

import (
	"bufio"
	"bytes"
	"context"
	"io"
	"io/ioutil"
	"os"
	"strconv"

	c "github.com/codenotary/immudb/cmd/helper"
	"github.com/spf13/cobra"
)

func (cl *commandline) rawSafeSet(cmd *cobra.Command) {
	ccmd := &cobra.Command{
		Use:               "rawsafeset key value",
		Short:             "Set a value for the item having the specified key, without setup structured values",
		Aliases:           []string{"rs"},
		PersistentPreRunE: cl.connect,
		PersistentPostRun: cl.disconnect,
		RunE: func(cmd *cobra.Command, args []string) error {

			key, err := ioutil.ReadAll(bytes.NewReader([]byte(args[0])))
			if err != nil {
				c.QuitToStdErr(err)
			}
			val, err := ioutil.ReadAll(bytes.NewReader([]byte(args[1])))
			if err != nil {
				c.QuitToStdErr(err)
			}

			ctx := context.Background()
			_, err = cl.ImmuClient.RawSafeSet(ctx, key, val)
			if err != nil {
				c.QuitWithUserError(err)
			}
			vi, err := cl.ImmuClient.RawSafeGet(ctx, key)

			printItem(vi.Key, vi.Value, vi)

			if err != nil {
				c.QuitWithUserError(err)
			}
			return nil
		},
		Args: cobra.ExactArgs(2),
	}
	cmd.AddCommand(ccmd)
}

func (cl *commandline) set(cmd *cobra.Command) {
	ccmd := &cobra.Command{
		Use:               "set key value",
		Short:             "Add new item having the specified key and value",
		Aliases:           []string{"s"},
		PersistentPreRunE: cl.connect,
		PersistentPostRun: cl.disconnect,
		RunE: func(cmd *cobra.Command, args []string) error {

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
				c.QuitToStdErr(err)
			}
			value, err := ioutil.ReadAll(tee)
			if err != nil {
				c.QuitToStdErr(err)
			}
			ctx := context.Background()
			_, err = cl.ImmuClient.Set(ctx, key, value)
			if err != nil {
				c.QuitWithUserError(err)
			}
			value2, err := ioutil.ReadAll(&buf)
			if err != nil {
				c.QuitToStdErr(err)
			}
			i, err := cl.ImmuClient.Get(ctx, key)
			if err != nil {
				c.QuitToStdErr(err)
			}
			printItem([]byte(args[0]), value2, i)
			return nil
		},
		Args: cobra.ExactArgs(2),
	}

	cmd.AddCommand(ccmd)
}

func (cl *commandline) safeset(cmd *cobra.Command) {
	ccmd := &cobra.Command{
		Use:               "safeset key value",
		Short:             "Add and verify new item having the specified key and value",
		Aliases:           []string{"ss"},
		PersistentPreRunE: cl.connect,
		PersistentPostRun: cl.disconnect,
		RunE: func(cmd *cobra.Command, args []string) error {

			var reader io.Reader
			if len(args) > 1 {
				reader = bytes.NewReader([]byte(args[1]))
			} else {
				reader = bufio.NewReader(os.Stdin)
			}
			key, err := ioutil.ReadAll(bytes.NewReader([]byte(args[0])))
			if err != nil {
				c.QuitToStdErr(err)
			}
			var buf bytes.Buffer
			tee := io.TeeReader(reader, &buf)
			value, err := ioutil.ReadAll(tee)
			if err != nil {
				c.QuitToStdErr(err)
			}
			ctx := context.Background()
			_, err = cl.ImmuClient.SafeSet(ctx, key, value)
			if err != nil {
				c.QuitWithUserError(err)
			}
			value2, err := ioutil.ReadAll(&buf)
			if err != nil {
				c.QuitToStdErr(err)
			}
			vi, err := cl.ImmuClient.SafeGet(ctx, key)
			if err != nil {
				c.QuitToStdErr(err)
			}
			printItem([]byte(args[0]), value2, vi)
			return nil
		},
		Args: cobra.ExactArgs(2),
	}
	cmd.AddCommand(ccmd)
}
func (cl *commandline) zAdd(cmd *cobra.Command) {
	ccmd := &cobra.Command{
		Use:               "zadd setname score key",
		Short:             "Add new key with score to a new or existing sorted set",
		Aliases:           []string{"za"},
		PersistentPreRunE: cl.connect,
		PersistentPostRun: cl.disconnect,
		RunE: func(cmd *cobra.Command, args []string) error {
			var setReader io.Reader
			var scoreReader io.Reader
			var keyReader io.Reader
			if len(args) > 1 {
				setReader = bytes.NewReader([]byte(args[0]))
				scoreReader = bytes.NewReader([]byte(args[1]))
				keyReader = bytes.NewReader([]byte(args[2]))
			}

			bs, err := ioutil.ReadAll(scoreReader)
			score, err := strconv.ParseFloat(string(bs[:]), 64)
			if err != nil {
				c.QuitToStdErr(err)
			}
			set, err := ioutil.ReadAll(setReader)
			if err != nil {
				c.QuitToStdErr(err)
			}
			key, err := ioutil.ReadAll(keyReader)
			if err != nil {
				c.QuitToStdErr(err)
			}
			ctx := context.Background()
			response, err := cl.ImmuClient.ZAdd(ctx, set, score, key)
			if err != nil {
				c.QuitWithUserError(err)
			}
			printSetItem([]byte(args[0]), []byte(args[2]), score, response)
			return nil
		},
		Args: cobra.MinimumNArgs(3),
	}
	cmd.AddCommand(ccmd)
}

func (cl *commandline) safeZAdd(cmd *cobra.Command) {
	ccmd := &cobra.Command{
		Use:               "safezadd setname score key",
		Short:             "Add and verify new key with score to a new or existing sorted set",
		Aliases:           []string{"sza"},
		PersistentPreRunE: cl.connect,
		PersistentPostRun: cl.disconnect,
		RunE: func(cmd *cobra.Command, args []string) error {
			var setReader io.Reader
			var scoreReader io.Reader
			var keyReader io.Reader
			if len(args) > 1 {
				setReader = bytes.NewReader([]byte(args[0]))
				scoreReader = bytes.NewReader([]byte(args[1]))
				keyReader = bytes.NewReader([]byte(args[2]))
			}
			bs, err := ioutil.ReadAll(scoreReader)
			score, err := strconv.ParseFloat(string(bs[:]), 64)
			if err != nil {
				c.QuitToStdErr(err)
			}
			set, err := ioutil.ReadAll(setReader)
			if err != nil {
				c.QuitToStdErr(err)
			}
			key, err := ioutil.ReadAll(keyReader)
			if err != nil {
				c.QuitToStdErr(err)
			}
			ctx := context.Background()
			response, err := cl.ImmuClient.SafeZAdd(ctx, set, score, key)
			if err != nil {
				c.QuitWithUserError(err)
			}
			printSetItem([]byte(args[0]), []byte(args[2]), score, response)
			return nil
		},
		Args: cobra.MinimumNArgs(3),
	}
	cmd.AddCommand(ccmd)
}

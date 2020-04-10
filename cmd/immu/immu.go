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

package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/hex"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"strconv"
	"time"

	c "github.com/codenotary/immudb/cmd"
	"github.com/codenotary/immudb/pkg/auth"
	"github.com/codenotary/immudb/pkg/client/timestamp"
	"github.com/codenotary/immudb/pkg/gw"
	"github.com/spf13/viper"

	"github.com/codenotary/immudb/cmd/docs/man"
	"github.com/codenotary/immudb/pkg/store"
	"google.golang.org/grpc"

	"github.com/codenotary/immudb/pkg/api"

	"github.com/spf13/cobra"

	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/codenotary/immudb/pkg/client"
)

var o = c.Options{}

var tokenFilename = "token"

func init() {
	cobra.OnInitialize(func() { o.InitConfig("immu") })
}

func main() {
	cmd := &cobra.Command{
		Use: "immu",
	}
	commands := []*cobra.Command{
		&cobra.Command{
			Use:     "login username \"password\"",
			Short:   fmt.Sprintf("Login using the specified username and \"password\" (username is \"%s\")", auth.AdminUser.Username),
			Aliases: []string{"l"},
			RunE: func(cmd *cobra.Command, args []string) error {
				options, err := options(cmd)
				if err != nil {
					c.QuitToStdErr(err)
				}
				immuClient := client.
					DefaultClient().
					WithOptions(*options)
				user := []byte(args[0])
				pass := []byte(args[1])
				ctx := context.Background()
				response, err := immuClient.Connected(ctx, func() (interface{}, error) {
					return immuClient.Login(ctx, user, pass)
				})
				if err != nil {
					c.QuitWithUserError(err)
				}
				if err := ioutil.WriteFile(tokenFilename, response.(*schema.LoginResponse).Token, 0644); err != nil {
					c.QuitToStdErr(err)
				}
				fmt.Printf("logged in\n")
				return nil
			},
			Args: cobra.ExactArgs(2),
		},
		&cobra.Command{
			Use:     "logout",
			Aliases: []string{"x"},
			RunE: func(cmd *cobra.Command, args []string) error {
				os.Remove(tokenFilename)
				fmt.Println("logged out")
				return nil
			},
			Args: cobra.NoArgs,
		},
		&cobra.Command{
			Use:     "get key",
			Short:   "Get item having the specified key",
			Aliases: []string{"g"},
			RunE: func(cmd *cobra.Command, args []string) error {

				key, err := ioutil.ReadAll(bytes.NewReader([]byte(args[0])))
				if err != nil {
					c.QuitToStdErr(err)
				}
				ctx := context.Background()
				immuClient := getImmuClient(cmd)
				response, err := immuClient.Connected(ctx, func() (interface{}, error) {
					return immuClient.Get(ctx, key)
				})
				if err != nil {
					c.QuitWithUserError(err)
				}
				printItem([]byte(args[0]), nil, response)
				return nil
			},
			Args: cobra.ExactArgs(1),
		},
		&cobra.Command{
			Use:     "safeget key",
			Short:   "Get and verify item having the specified key",
			Aliases: []string{"sg"},
			RunE: func(cmd *cobra.Command, args []string) error {

				key, err := ioutil.ReadAll(bytes.NewReader([]byte(args[0])))
				if err != nil {
					c.QuitToStdErr(err)
				}
				ctx := context.Background()
				immuClient := getImmuClient(cmd)
				response, err := immuClient.Connected(ctx, func() (interface{}, error) {
					return immuClient.SafeGet(ctx, key)
				})
				if err != nil {
					c.QuitWithUserError(err)
				}
				printItem([]byte(args[0]), nil, response)
				return nil
			},
			Args: cobra.ExactArgs(1),
		},
		&cobra.Command{
			Use:     "set key value",
			Short:   "Add new item having the specified key and value",
			Aliases: []string{"s"},
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
				immuClient := getImmuClient(cmd)
				response, err := immuClient.Connected(ctx, func() (interface{}, error) {
					return immuClient.Set(ctx, key, value)
				})
				if err != nil {
					c.QuitWithUserError(err)
				}
				value2, err := ioutil.ReadAll(&buf)
				if err != nil {
					c.QuitToStdErr(err)
				}
				response, err = immuClient.Connected(ctx, func() (interface{}, error) {
					return immuClient.Get(ctx, key)
				})
				if err != nil {
					c.QuitToStdErr(err)
				}
				printItem([]byte(args[0]), value2, response)
				return nil
			},
			Args: cobra.MinimumNArgs(1),
		},
		&cobra.Command{
			Use:     "safeset key value",
			Short:   "Add and verify new item having the specified key and value",
			Aliases: []string{"ss"},
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
				immuClient := getImmuClient(cmd)
				response, err := immuClient.Connected(ctx, func() (interface{}, error) {
					return immuClient.SafeSet(ctx, key, value)
				})
				if err != nil {
					c.QuitWithUserError(err)
				}
				value2, err := ioutil.ReadAll(&buf)
				if err != nil {
					c.QuitToStdErr(err)
				}
				response, err = immuClient.Connected(ctx, func() (interface{}, error) {
					return immuClient.Get(ctx, key)
				})
				if err != nil {
					c.QuitToStdErr(err)
				}
				printItem([]byte(args[0]), value2, response)
				return nil
			},
			Args: cobra.MinimumNArgs(1),
		},
		&cobra.Command{
			Use:     "reference refkey key",
			Short:   "Add new reference to an existing key",
			Aliases: []string{"r"},
			RunE: func(cmd *cobra.Command, args []string) error {

				var reader io.Reader
				if len(args) > 1 {
					reader = bytes.NewReader([]byte(args[1]))
				} else {
					reader = bufio.NewReader(os.Stdin)
				}
				reference, err := ioutil.ReadAll(bytes.NewReader([]byte(args[0])))
				if err != nil {
					c.QuitToStdErr(err)
				}
				var buf bytes.Buffer
				tee := io.TeeReader(reader, &buf)
				key, err := ioutil.ReadAll(tee)
				if err != nil {
					c.QuitToStdErr(err)
				}
				ctx := context.Background()
				immuClient := getImmuClient(cmd)
				response, err := immuClient.Connected(ctx, func() (interface{}, error) {
					return immuClient.Reference(ctx, reference, key)
				})
				if err != nil {
					c.QuitWithUserError(err)
				}
				value, err := ioutil.ReadAll(&buf)
				if err != nil {
					c.QuitToStdErr(err)
				}
				printItem([]byte(args[0]), value, response)
				return nil
			},
			Args: cobra.MinimumNArgs(1),
		},
		&cobra.Command{
			Use:     "safereference refkey key",
			Short:   "Add and verify new reference to an existing key",
			Aliases: []string{"sr"},
			RunE: func(cmd *cobra.Command, args []string) error {

				var reader io.Reader
				if len(args) > 1 {
					reader = bytes.NewReader([]byte(args[1]))
				} else {
					reader = bufio.NewReader(os.Stdin)
				}
				reference, err := ioutil.ReadAll(bytes.NewReader([]byte(args[0])))
				if err != nil {
					c.QuitToStdErr(err)
				}
				var buf bytes.Buffer
				tee := io.TeeReader(reader, &buf)
				key, err := ioutil.ReadAll(tee)
				if err != nil {
					c.QuitToStdErr(err)
				}
				ctx := context.Background()
				immuClient := getImmuClient(cmd)
				response, err := immuClient.Connected(ctx, func() (interface{}, error) {
					return immuClient.SafeReference(ctx, reference, key)
				})
				if err != nil {
					c.QuitWithUserError(err)
				}
				value, err := ioutil.ReadAll(&buf)
				if err != nil {
					c.QuitToStdErr(err)
				}
				printItem([]byte(args[0]), value, response)
				return nil
			},
			Args: cobra.MinimumNArgs(1),
		},
		&cobra.Command{
			Use:     "zadd setname score key",
			Short:   "Add new key with score to a new or existing sorted set",
			Aliases: []string{"za"},
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
				immuClient := getImmuClient(cmd)
				response, err := immuClient.Connected(ctx, func() (interface{}, error) {
					return immuClient.ZAdd(ctx, set, score, key)
				})
				if err != nil {
					c.QuitWithUserError(err)
				}
				printSetItem([]byte(args[0]), []byte(args[2]), score, response)
				return nil
			},
			Args: cobra.MinimumNArgs(3),
		},
		&cobra.Command{
			Use:     "safezadd setname score key",
			Short:   "Add and verify new key with score to a new or existing sorted set",
			Aliases: []string{"sza"},
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
				immuClient := getImmuClient(cmd)
				response, err := immuClient.Connected(ctx, func() (interface{}, error) {
					return immuClient.SafeZAdd(ctx, set, score, key)
				})
				if err != nil {
					c.QuitWithUserError(err)
				}
				printSetItem([]byte(args[0]), []byte(args[2]), score, response)
				return nil
			},
			Args: cobra.MinimumNArgs(3),
		},
		&cobra.Command{
			Use:     "zscan setname",
			Short:   "Iterate over a sorted set",
			Aliases: []string{"zscn"},
			RunE: func(cmd *cobra.Command, args []string) error {
				set, err := ioutil.ReadAll(bytes.NewReader([]byte(args[0])))
				if err != nil {
					c.QuitToStdErr(err)
				}
				ctx := context.Background()
				immuClient := getImmuClient(cmd)
				response, err := immuClient.Connected(ctx, func() (interface{}, error) {
					return immuClient.ZScan(ctx, set)
				})
				if err != nil {
					c.QuitWithUserError(err)
				}
				for _, item := range response.(*schema.StructuredItemList).Items {
					printItem(nil, nil, item)
					fmt.Println()
				}
				return nil
			},
			Args: cobra.ExactArgs(1),
		},
		&cobra.Command{
			Use:     "scan prefix",
			Short:   "Iterate over keys having the specified prefix",
			Aliases: []string{"scn"},
			RunE: func(cmd *cobra.Command, args []string) error {

				prefix, err := ioutil.ReadAll(bytes.NewReader([]byte(args[0])))
				if err != nil {
					c.QuitToStdErr(err)
				}
				ctx := context.Background()
				immuClient := getImmuClient(cmd)
				response, err := immuClient.Connected(ctx, func() (interface{}, error) {
					return immuClient.Scan(ctx, prefix)
				})
				if err != nil {
					c.QuitWithUserError(err)
				}
				for _, item := range response.(*schema.StructuredItemList).Items {
					printItem(nil, nil, item)
					fmt.Println()
				}
				return nil
			},
			Args: cobra.ExactArgs(1),
		},
		&cobra.Command{
			Use:     "count prefix",
			Short:   "Count keys having the specified prefix",
			Aliases: []string{"cnt"},
			RunE: func(cmd *cobra.Command, args []string) error {

				prefix, err := ioutil.ReadAll(bytes.NewReader([]byte(args[0])))
				if err != nil {
					c.QuitToStdErr(err)
				}
				ctx := context.Background()
				immuClient := getImmuClient(cmd)
				response, err := immuClient.Connected(ctx, func() (interface{}, error) {
					return immuClient.Count(ctx, prefix)
				})
				if err != nil {
					c.QuitWithUserError(err)
				}
				fmt.Println(response.(*schema.ItemsCount).Count)
				return nil
			},
			Args: cobra.ExactArgs(1),
		},
		&cobra.Command{
			Use:     "inclusion index",
			Short:   "Check if specified index is included in the current tree",
			Aliases: []string{"i"},
			RunE: func(cmd *cobra.Command, args []string) error {
				index, err := strconv.ParseUint(args[0], 10, 64)
				if err != nil {
					c.QuitToStdErr(err)
				}
				ctx := context.Background()
				immuClient := getImmuClient(cmd)
				response, err := immuClient.Connected(ctx, func() (interface{}, error) {
					return immuClient.Inclusion(ctx, index)
				})
				if err != nil {
					c.QuitWithUserError(err)
				}

				proof := response.(*schema.InclusionProof)

				var hash []byte
				if len(args) > 1 {
					src := []byte(args[1])
					l := hex.DecodedLen(len(src))
					if l != 32 {
						c.QuitToStdErr(fmt.Errorf("invalid hash length"))
					}
					hash = make([]byte, l)
					_, err := hex.Decode(hash, src)
					if err != nil {
						c.QuitToStdErr(err)
					}

				} else {
					response, err := immuClient.Connected(ctx, func() (interface{}, error) {
						return immuClient.ByIndex(ctx, index)
					})
					if err != nil {
						c.QuitWithUserError(err)
					}
					item := response.(*schema.StructuredItem)
					hash, err = item.Hash()

				}

				fmt.Printf(`verified: %t

hash: %x at index: %d
root: %x at index: %d

`, proof.Verify(index, hash), proof.Leaf, proof.Index, proof.Root, proof.At)
				return nil
			},
			Args: cobra.MinimumNArgs(1),
		},
		&cobra.Command{
			Use:     "verify index hash",
			Short:   "Check consistency for the specified index and hash",
			Aliases: []string{"c"},
			RunE: func(cmd *cobra.Command, args []string) error {
				index, err := strconv.ParseUint(args[0], 10, 64)
				if err != nil {
					c.QuitToStdErr(err)
				}
				ctx := context.Background()
				immuClient := getImmuClient(cmd)
				response, err := immuClient.Connected(ctx, func() (interface{}, error) {
					return immuClient.Consistency(ctx, index)
				})
				if err != nil {
					c.QuitWithUserError(err)
				}

				proof := response.(*schema.ConsistencyProof)

				var root []byte
				src := []byte(args[1])
				l := hex.DecodedLen(len(src))
				if l != 32 {
					c.QuitToStdErr(fmt.Errorf("invalid hash length"))
				}
				root = make([]byte, l)
				_, err = hex.Decode(root, src)
				if err != nil {
					c.QuitToStdErr(err)
				}

				fmt.Printf(`verified: %t

firstRoot: %x at index: %d
secondRoot: %x at index: %d

`, proof.Verify(schema.Root{Index: index, Root: root}), proof.FirstRoot, proof.First, proof.SecondRoot, proof.Second)
				return nil
			},
			Args: cobra.MinimumNArgs(2),
		},
		&cobra.Command{
			Use:     "history key",
			Short:   "Fetch history for the item having the specified key",
			Aliases: []string{"h"},
			RunE: func(cmd *cobra.Command, args []string) error {

				key, err := ioutil.ReadAll(bytes.NewReader([]byte(args[0])))
				if err != nil {
					c.QuitToStdErr(err)
				}
				ctx := context.Background()
				immuClient := getImmuClient(cmd)
				response, err := immuClient.Connected(ctx, func() (interface{}, error) {
					return immuClient.History(ctx, key)
				})
				if err != nil {
					c.QuitWithUserError(err)
				}
				for _, item := range response.(*schema.StructuredItemList).Items {
					printItem(nil, nil, item)
					fmt.Println()
				}
				return nil
			},
			Args: cobra.ExactArgs(1),
		},
		&cobra.Command{
			Use:     "ping",
			Short:   "Ping to check if server connection is alive",
			Aliases: []string{"p"},
			RunE: func(cmd *cobra.Command, args []string) error {

				ctx := context.Background()
				immuClient := getImmuClient(cmd)
				_, err := immuClient.Connected(ctx, func() (interface{}, error) {
					return nil, immuClient.HealthCheck(ctx)
				})
				if err != nil {
					c.QuitWithUserError(err)
				}
				fmt.Println("Health check OK")
				return nil
			},
			Args: cobra.NoArgs,
		},
		&cobra.Command{
			Use:     "backup [filename]",
			Short:   "Save a backup to the specified filename (optional)",
			Aliases: []string{"b"},
			RunE: func(cmd *cobra.Command, args []string) error {

				filename := fmt.Sprint("immudb_" + time.Now().Format("2006-01-02_15-04-05") + ".bkp")
				if len(args) > 0 {
					filename = args[0]
				}
				file, err := os.Create(filename)
				defer file.Close()
				if err != nil {
					c.QuitToStdErr(err)
				}
				ctx := context.Background()
				immuClient := getImmuClient(cmd)
				response, err := immuClient.Connected(ctx, func() (interface{}, error) {
					return immuClient.Backup(ctx, file)
				})
				if err != nil {
					c.QuitWithUserError(err)
				}
				fmt.Printf("SUCCESS: %d key-value entries were backed-up to file %s\n", response.(int64), filename)
				return nil
			},
			Args: cobra.MaximumNArgs(1),
		},
		&cobra.Command{
			Use:     "restore filename",
			Short:   "Restore a backup from the specified filename",
			Aliases: []string{"rb"},
			RunE: func(cmd *cobra.Command, args []string) error {
				file, err := os.Open(args[0])
				if err != nil {
					c.QuitToStdErr(err)
				}
				defer file.Close()
				if err != nil {
					c.QuitToStdErr(err)
				}
				ctx := context.Background()
				immuClient := getImmuClient(cmd)
				response, err := immuClient.Connected(ctx, func() (interface{}, error) {
					return immuClient.Restore(ctx, file, 500)
				})
				if err != nil {
					c.QuitWithUserError(err)
				}
				fmt.Printf("SUCCESS: %d key-value entries were restored from file %s\n", response.(int64), args[0])
				return nil
			},
			Args: cobra.ExactArgs(1),
		},
	}

	if err := configureOptions(cmd); err != nil {
		c.QuitToStdErr(err)
	}

	for _, command := range commands {
		cmd.AddCommand(command)
	}

	cmd.AddCommand(man.Generate(cmd, "immu", "../docs/man/immu"))

	if err := cmd.Execute(); err != nil {
		c.QuitToStdErr(err)
	}
}

func configureOptions(cmd *cobra.Command) error {
	cmd.PersistentFlags().IntP("port", "p", gw.DefaultOptions().ImmudPort, "immudb port number")
	cmd.PersistentFlags().StringP("address", "a", gw.DefaultOptions().ImmudAddress, "immudb host address")
	cmd.PersistentFlags().StringVar(&o.CfgFn, "config", "", "config file (default path are config or $HOME. Default filename is immu.ini)")
	cmd.PersistentFlags().BoolP("auth", "s", client.DefaultOptions().Auth, "use authentication")
	if err := viper.BindPFlag("default.port", cmd.PersistentFlags().Lookup("port")); err != nil {
		return err
	}
	if err := viper.BindPFlag("default.address", cmd.PersistentFlags().Lookup("address")); err != nil {
		return err
	}
	if err := viper.BindPFlag("default.auth", cmd.PersistentFlags().Lookup("auth")); err != nil {
		return err
	}
	viper.SetDefault("default.port", gw.DefaultOptions().ImmudPort)
	viper.SetDefault("default.address", gw.DefaultOptions().ImmudAddress)
	viper.SetDefault("default.auth", client.DefaultOptions().Auth)
	return nil
}

func getImmuClient(cmd *cobra.Command) *client.ImmuClient {
	options, err := options(cmd)
	if err != nil {
		c.QuitToStdErr(err)
	}
	dt, err := timestamp.NewTdefault()
	if err != nil {
		c.QuitToStdErr(err)
	}
	ts := client.NewTimestampService(dt)
	immuClient := client.
		DefaultClient().
		WithOptions(*options).
		WithTimestampService(ts)
	return immuClient
}

func options(cmd *cobra.Command) (*client.Options, error) {
	port := viper.GetInt("default.port")
	address := viper.GetString("default.address")
	authEnabled := viper.GetBool("default.auth")
	options := client.DefaultOptions().
		WithPort(port).
		WithAddress(address).
		WithAuth(authEnabled).
		WithDialOptions(false, grpc.WithInsecure())
	if authEnabled {
		tokenBytes, err := ioutil.ReadFile(tokenFilename)
		if err == nil {
			token := string(tokenBytes)
			options = options.WithDialOptions(
				false,
				grpc.WithUnaryInterceptor(auth.ClientUnaryInterceptor(token)),
				grpc.WithStreamInterceptor(auth.ClientStreamInterceptor(token)),
			)
		}
	}

	return &options, nil
}

func printItem(key []byte, value []byte, message interface{}) {
	var index uint64
	verified := false
	isVerified := false
	var ts uint64
	var hash []byte
	switch m := message.(type) {
	case *schema.Index:
		index = m.Index
		dig := api.Digest(index, key, value)
		hash = dig[:]
	case *client.VerifiedIndex:
		index = m.Index
		dig := api.Digest(index, key, value)
		hash = dig[:]
		verified = m.Verified
		isVerified = true
	case *schema.Item:
		key = m.Key
		value = m.Value
		index = m.Index
		hash = m.Hash()
	case *schema.StructuredItem:
		key = m.Key
		value = m.Value.Payload
		ts = m.Value.Timestamp
		index = m.Index
		hash, _ = m.Hash()
	case *client.VerifiedItem:
		key = m.Key
		value = m.Value
		index = m.Index
		ts = m.Time
		verified = m.Verified
		isVerified = true
		me, _ := schema.Merge(value, ts)
		dig := api.Digest(index, key, me)
		hash = dig[:]

	}
	if !isVerified {
		fmt.Printf(`index:		%d
key:		%s
value:		%s
hash:		%x
time:		%s
`, index, key, value, hash, time.Unix(int64(ts), 0))
		return
	}
	fmt.Printf(`index:		%d
key:		%s
value:		%s
hash:		%x
time:		%s
verified:	%t
`, index, key, value, hash, time.Unix(int64(ts), 0), verified)
}

func printSetItem(set []byte, rkey []byte, score float64, message interface{}) {
	var index uint64
	verified := false
	isVerified := false
	switch m := message.(type) {
	case *schema.Index:
		index = m.Index
	case *client.VerifiedIndex:
		index = m.Index
		verified = m.Verified
		isVerified = true
	}
	key, err := store.SetKey(rkey, set, score)
	if err != nil {
		fmt.Printf(err.Error())
	}
	if !isVerified {
		fmt.Printf(`index:		%d
set:		%s
key:		%s
score:		%f
value:		%s
hash:		%x
`, index, set, key, score, rkey, api.Digest(index, key, rkey))
		return
	}
	fmt.Printf(`index:		%d
set:		%s
key:		%s
score:		%f
value:		%s
hash:		%x
verified:	%t
`, index, set, key, score, rkey, api.Digest(index, key, rkey), verified)
}

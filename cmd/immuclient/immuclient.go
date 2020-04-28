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
	"github.com/codenotary/immudb/pkg/gw"
	"github.com/spf13/viper"

	"github.com/codenotary/immudb/cmd/docs/man"
	"github.com/codenotary/immudb/pkg/api"
	"github.com/codenotary/immudb/pkg/store"

	"github.com/spf13/cobra"

	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/codenotary/immudb/pkg/client"
)

var o = c.Options{}
var immuClient client.ImmuClient

func init() {
	cobra.OnInitialize(func() { o.InitConfig("immuclient") })
}

func main() {

	cmd := &cobra.Command{
		Use:   "immuclient",
		Short: "CLI client for the ImmuDB tamperproof database",
		Long: `CLI client for the ImmuDB tamperproof database.

Environment variables:
  IMMUCLIENT_ADDRESS=127.0.0.1
  IMMUCLIENT_PORT=3322
  IMMUCLIENT_AUTH=false`,
		DisableAutoGenTag: true,
		PersistentPreRunE: func(cmd *cobra.Command, args []string) (err error) {
			if immuClient, err = client.NewImmuClient(options()); err != nil {
				c.QuitToStdErr(err)
			}
			return
		},
		PersistentPostRun: func(cmd *cobra.Command, args []string) {
			if err := immuClient.Disconnect(); err != nil {
				c.QuitToStdErr(err)
			}
		},
	}
	commands := []*cobra.Command{
		{
			Use:     "login username \"password\"",
			Short:   fmt.Sprintf("Login using the specified username and \"password\" (username is \"%s\")", auth.AdminUser.Username),
			Aliases: []string{"l"},
			RunE: func(cmd *cobra.Command, args []string) error {
				user := []byte(args[0])
				pass := []byte(args[1])
				ctx := context.Background()
				response, err := immuClient.Login(ctx, user, pass)
				if err != nil {
					c.QuitWithUserError(err)
				}
				if err := ioutil.WriteFile(client.TokenFileName, response.Token, 0644); err != nil {
					c.QuitToStdErr(err)
				}
				fmt.Printf("logged in\n")
				return nil
			},
			Args: cobra.ExactArgs(2),
		},
		{
			Use:     "current",
			Short:   "Return the last merkle tree root and index stored locally",
			Aliases: []string{"crt"},
			RunE: func(cmd *cobra.Command, args []string) error {
				ctx := context.Background()
				root, err := immuClient.CurrentRoot(ctx)
				if err != nil {
					c.QuitWithUserError(err)
				}
				printRoot(root)
				return nil
			},
			Args: cobra.ExactArgs(0),
		},
		{
			Use:     "getByIndex",
			Short:   "Return an element by index",
			Aliases: []string{"bi"},
			RunE: func(cmd *cobra.Command, args []string) error {
				index, err := strconv.ParseUint(args[0], 10, 64)
				if err != nil {
					c.QuitToStdErr(err)
				}
				ctx := context.Background()
				response, err := immuClient.ByIndex(ctx, index)
				if err != nil {
					c.QuitWithUserError(err)
				}
				printByIndex(response)
				return nil
			},
			Args: cobra.ExactArgs(1),
		},
		{
			Use:     "logout",
			Aliases: []string{"x"},
			RunE: func(cmd *cobra.Command, args []string) error {
				if err := os.Remove(client.TokenFileName); err != nil {
					c.QuitWithUserError(err)
				}
				fmt.Println("logged out")
				return nil
			},
			Args: cobra.NoArgs,
		},
		{
			Use:     "get key",
			Short:   "Get item having the specified key",
			Aliases: []string{"g"},
			RunE: func(cmd *cobra.Command, args []string) error {

				key, err := ioutil.ReadAll(bytes.NewReader([]byte(args[0])))
				if err != nil {
					c.QuitToStdErr(err)
				}
				ctx := context.Background()
				response, err := immuClient.Get(ctx, key)
				if err != nil {
					c.QuitWithUserError(err)
				}
				printItem([]byte(args[0]), nil, response)
				return nil
			},
			Args: cobra.ExactArgs(1),
		},
		{
			Use:     "rawsafeget key",
			Short:   "Get item having the specified key, without parsing structured values",
			Aliases: []string{"rg"},
			RunE: func(cmd *cobra.Command, args []string) error {
				key, err := ioutil.ReadAll(bytes.NewReader([]byte(args[0])))
				if err != nil {
					c.QuitToStdErr(err)
				}
				ctx := context.Background()
				vi, err := immuClient.RawSafeGet(ctx, key)
				printItem(vi.Key, vi.Value, vi)
				return nil
			},
			Args: cobra.ExactArgs(1),
		},
		{
			Use:     "rawsafeset key",
			Short:   "Set item having the specified key, without setup structured values",
			Aliases: []string{"rs"},
			RunE: func(cmd *cobra.Command, args []string) error {

				key, err := ioutil.ReadAll(bytes.NewReader([]byte(args[0])))
				if err != nil {
					c.QuitToStdErr(err)
				}
				val, err := ioutil.ReadAll(bytes.NewReader([]byte(args[0])))
				if err != nil {
					c.QuitToStdErr(err)
				}

				ctx := context.Background()
				_, err = immuClient.RawSafeSet(ctx, key, val)
				if err != nil {
					c.QuitWithUserError(err)
				}
				vi, err := immuClient.RawSafeGet(ctx, key)

				printItem(vi.Key, vi.Value, vi)

				if err != nil {
					c.QuitWithUserError(err)
				}
				return nil
			},
			Args: cobra.ExactArgs(2),
		},
		{
			Use:     "safeget key",
			Short:   "Get and verify item having the specified key",
			Aliases: []string{"sg"},
			RunE: func(cmd *cobra.Command, args []string) error {

				key, err := ioutil.ReadAll(bytes.NewReader([]byte(args[0])))
				if err != nil {
					c.QuitToStdErr(err)
				}
				ctx := context.Background()
				response, err := immuClient.SafeGet(ctx, key)
				if err != nil {
					c.QuitWithUserError(err)
				}
				printItem([]byte(args[0]), nil, response)
				return nil
			},
			Args: cobra.ExactArgs(1),
		},
		{
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
				_, err = immuClient.Set(ctx, key, value)
				if err != nil {
					c.QuitWithUserError(err)
				}
				value2, err := ioutil.ReadAll(&buf)
				if err != nil {
					c.QuitToStdErr(err)
				}
				i, err := immuClient.Get(ctx, key)
				if err != nil {
					c.QuitToStdErr(err)
				}
				printItem([]byte(args[0]), value2, i)
				return nil
			},
			Args: cobra.ExactArgs(2),
		},
		{
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
				_, err = immuClient.SafeSet(ctx, key, value)
				if err != nil {
					c.QuitWithUserError(err)
				}
				value2, err := ioutil.ReadAll(&buf)
				if err != nil {
					c.QuitToStdErr(err)
				}
				vi, err := immuClient.SafeGet(ctx, key)
				if err != nil {
					c.QuitToStdErr(err)
				}
				printItem([]byte(args[0]), value2, vi)
				return nil
			},
			Args: cobra.ExactArgs(2),
		},
		{
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
				response, err := immuClient.Reference(ctx, reference, key)
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
		{
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
				response, err := immuClient.SafeReference(ctx, reference, key)
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
		{
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
				response, err := immuClient.ZAdd(ctx, set, score, key)
				if err != nil {
					c.QuitWithUserError(err)
				}
				printSetItem([]byte(args[0]), []byte(args[2]), score, response)
				return nil
			},
			Args: cobra.MinimumNArgs(3),
		},
		{
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
				response, err := immuClient.SafeZAdd(ctx, set, score, key)
				if err != nil {
					c.QuitWithUserError(err)
				}
				printSetItem([]byte(args[0]), []byte(args[2]), score, response)
				return nil
			},
			Args: cobra.MinimumNArgs(3),
		},
		{
			Use:     "zscan setname",
			Short:   "Iterate over a sorted set",
			Aliases: []string{"zscn"},
			RunE: func(cmd *cobra.Command, args []string) error {
				set, err := ioutil.ReadAll(bytes.NewReader([]byte(args[0])))
				if err != nil {
					c.QuitToStdErr(err)
				}
				ctx := context.Background()
				response, err := immuClient.ZScan(ctx, set)
				if err != nil {
					c.QuitWithUserError(err)
				}
				for _, item := range response.Items {
					printItem(nil, nil, item)
					fmt.Println()
				}
				return nil
			},
			Args: cobra.ExactArgs(1),
		},
		{
			Use:     "iscan pagenumber pagesize",
			Short:   "Iterate over all elements by insertion order",
			Aliases: []string{"iscn"},
			RunE: func(cmd *cobra.Command, args []string) error {
				pageNumber, err := strconv.ParseUint(args[0], 10, 64)
				if err != nil {
					c.QuitToStdErr(err)
				}
				pageSize, err := strconv.ParseUint(args[1], 10, 64)
				if err != nil {
					c.QuitToStdErr(err)
				}
				ctx := context.Background()
				response, err := immuClient.IScan(ctx, pageNumber, pageSize)
				if err != nil {
					c.QuitWithUserError(err)
				}
				for _, item := range response.Items {
					printItem(nil, nil, item)
					fmt.Println()
				}
				return nil
			},
			Args: cobra.ExactArgs(2),
		},
		{
			Use:     "scan prefix",
			Short:   "Iterate over keys having the specified prefix",
			Aliases: []string{"scn"},
			RunE: func(cmd *cobra.Command, args []string) error {

				prefix, err := ioutil.ReadAll(bytes.NewReader([]byte(args[0])))
				if err != nil {
					c.QuitToStdErr(err)
				}
				ctx := context.Background()
				response, err := immuClient.Scan(ctx, prefix)
				if err != nil {
					c.QuitWithUserError(err)
				}
				for _, item := range response.Items {
					printItem(nil, nil, item)
					fmt.Println()
				}
				return nil
			},
			Args: cobra.ExactArgs(1),
		},
		{
			Use:     "count prefix",
			Short:   "Count keys having the specified prefix",
			Aliases: []string{"cnt"},
			RunE: func(cmd *cobra.Command, args []string) error {

				prefix, err := ioutil.ReadAll(bytes.NewReader([]byte(args[0])))
				if err != nil {
					c.QuitToStdErr(err)
				}
				ctx := context.Background()
				response, err := immuClient.Count(ctx, prefix)
				if err != nil {
					c.QuitWithUserError(err)
				}
				fmt.Println(response.Count)
				return nil
			},
			Args: cobra.ExactArgs(1),
		},
		{
			Use:     "inclusion index",
			Short:   "Check if specified index is included in the current tree",
			Aliases: []string{"i"},
			RunE: func(cmd *cobra.Command, args []string) error {
				index, err := strconv.ParseUint(args[0], 10, 64)
				if err != nil {
					c.QuitToStdErr(err)
				}
				ctx := context.Background()
				proof, err := immuClient.Inclusion(ctx, index)
				if err != nil {
					c.QuitWithUserError(err)
				}
				var hash []byte
				if len(args) > 1 {
					src := []byte(args[1])
					l := hex.DecodedLen(len(src))
					if l != 32 {
						c.QuitToStdErr(fmt.Errorf("invalid hash length"))
					}
					hash = make([]byte, l)
					_, err = hex.Decode(hash, src)
					if err != nil {
						c.QuitToStdErr(err)
					}

				} else {
					item, err := immuClient.ByIndex(ctx, index)
					if err != nil {
						c.QuitWithUserError(err)
					}
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
		{
			Use:     "check-consistency index hash",
			Short:   "Check consistency for the specified index and hash",
			Aliases: []string{"c"},
			RunE: func(cmd *cobra.Command, args []string) error {
				index, err := strconv.ParseUint(args[0], 10, 64)
				if err != nil {
					c.QuitToStdErr(err)
				}
				ctx := context.Background()
				proof, err := immuClient.Consistency(ctx, index)
				if err != nil {
					c.QuitWithUserError(err)
				}

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
		{
			Use:     "history key",
			Short:   "Fetch history for the item having the specified key",
			Aliases: []string{"h"},
			RunE: func(cmd *cobra.Command, args []string) error {

				key, err := ioutil.ReadAll(bytes.NewReader([]byte(args[0])))
				if err != nil {
					c.QuitToStdErr(err)
				}
				ctx := context.Background()
				response, err := immuClient.History(ctx, key)
				if err != nil {
					c.QuitWithUserError(err)
				}
				for _, item := range response.Items {
					printItem(nil, nil, item)
					fmt.Println()
				}
				return nil
			},
			Args: cobra.ExactArgs(1),
		},
		{
			Use:     "ping",
			Short:   "Ping to check if server connection is alive",
			Aliases: []string{"p"},
			RunE: func(cmd *cobra.Command, args []string) error {

				ctx := context.Background()
				if err := immuClient.HealthCheck(ctx); err != nil {
					c.QuitWithUserError(err)
				}
				fmt.Println("Health check OK")
				return nil
			},
			Args: cobra.NoArgs,
		},
		{
			Use:     "dump [filename]",
			Short:   "Save a database dump to the specified filename (optional)",
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
				response, err := immuClient.Dump(ctx, file)
				if err != nil {
					c.QuitWithUserError(err)
				}
				fmt.Printf("SUCCESS: %d key-value entries were backed-up to file %s\n", response, filename)
				return nil
			},
			Args: cobra.MaximumNArgs(1),
		},
		// todo(joe-dz): Enable restore when the feature is required again.
		// Also, make sure that the generated files are updated
		//{
		//	Use:     "restore filename",
		//	Short:   "Restore a backup from the specified filename",
		//	Aliases: []string{"rb"},
		//	RunE: func(cmd *cobra.Command, args []string) error {
		//		file, err := os.Open(args[0])
		//		if err != nil {
		//			c.QuitToStdErr(err)
		//		}
		//		defer file.Close()
		//		if err != nil {
		//			c.QuitToStdErr(err)
		//		}
		//		ctx := context.Background()
		//	//		response, err := immuClient.Connected(ctx, func() (interface{}, error) {
		//			return immuClient.Restore(ctx, file, 500)
		//		})
		//		if err != nil {
		//			c.QuitWithUserError(err)
		//		}
		//		fmt.Printf("SUCCESS: %d key-value entries were restored from file %s\n", response.(int64), args[0])
		//		return nil
		//	},
		//	Args: cobra.ExactArgs(1),
		//},
	}

	if err := configureOptions(cmd); err != nil {
		c.QuitToStdErr(err)
	}

	for _, command := range commands {
		cmd.AddCommand(command)
	}

	cmd.AddCommand(man.Generate(cmd, "immuclient", "../docs/man/immuclient"))

	if err := cmd.Execute(); err != nil {
		c.QuitToStdErr(err)
	}
}

func configureOptions(cmd *cobra.Command) error {
	cmd.PersistentFlags().IntP("port", "p", gw.DefaultOptions().ImmudbPort, "immudb port number")
	cmd.PersistentFlags().StringP("address", "a", gw.DefaultOptions().ImmudbAddress, "immudb host address")
	cmd.PersistentFlags().StringVar(&o.CfgFn, "config", "", "config file (default path are configs or $HOME. Default filename is immuclient.ini)")
	cmd.PersistentFlags().BoolP("auth", "s", client.DefaultOptions().Auth, "use authentication")
	cmd.PersistentFlags().BoolP("mtls", "m", client.DefaultOptions().MTLs, "enable mutual tls")
	cmd.PersistentFlags().String("servername", client.DefaultMTLsOptions().Servername, "used to verify the hostname on the returned certificates")
	cmd.PersistentFlags().String("certificate", client.DefaultMTLsOptions().Certificate, "server certificate file path")
	cmd.PersistentFlags().String("pkey", client.DefaultMTLsOptions().Pkey, "server private key path")
	cmd.PersistentFlags().String("clientcas", client.DefaultMTLsOptions().ClientCAs, "clients certificates list. Aka certificate authority")
	if err := viper.BindPFlag("default.port", cmd.PersistentFlags().Lookup("port")); err != nil {
		return err
	}
	if err := viper.BindPFlag("default.address", cmd.PersistentFlags().Lookup("address")); err != nil {
		return err
	}
	if err := viper.BindPFlag("default.auth", cmd.PersistentFlags().Lookup("auth")); err != nil {
		return err
	}
	if err := viper.BindPFlag("default.mtls", cmd.PersistentFlags().Lookup("mtls")); err != nil {
		return err
	}
	if err := viper.BindPFlag("default.servername", cmd.PersistentFlags().Lookup("servername")); err != nil {
		return err
	}
	if err := viper.BindPFlag("default.certificate", cmd.PersistentFlags().Lookup("certificate")); err != nil {
		return err
	}
	if err := viper.BindPFlag("default.pkey", cmd.PersistentFlags().Lookup("pkey")); err != nil {
		return err
	}
	if err := viper.BindPFlag("default.clientcas", cmd.PersistentFlags().Lookup("clientcas")); err != nil {
		return err
	}
	viper.SetDefault("default.port", gw.DefaultOptions().ImmudbPort)
	viper.SetDefault("default.address", gw.DefaultOptions().ImmudbAddress)
	viper.SetDefault("default.auth", client.DefaultOptions().Auth)
	viper.SetDefault("default.mtls", client.DefaultOptions().MTLs)
	viper.SetDefault("default.servername", client.DefaultMTLsOptions().Servername)
	viper.SetDefault("default.certificate", client.DefaultMTLsOptions().Certificate)
	viper.SetDefault("default.pkey", client.DefaultMTLsOptions().Pkey)
	viper.SetDefault("default.clientcas", client.DefaultMTLsOptions().ClientCAs)

	return nil
}

func options() *client.Options {
	port := viper.GetInt("default.port")
	address := viper.GetString("default.address")
	authEnabled := viper.GetBool("default.auth")
	mtls := viper.GetBool("default.mtls")
	certificate := viper.GetString("default.certificate")
	servername := viper.GetString("default.servername")
	pkey := viper.GetString("default.pkey")
	clientcas := viper.GetString("default.clientcas")
	options := client.DefaultOptions().
		WithPort(port).
		WithAddress(address).
		WithAuth(authEnabled).
		WithMTLs(mtls)
	if mtls {
		// todo https://golang.org/src/crypto/x509/root_linux.go
		options.MTLsOptions = client.DefaultMTLsOptions().
			WithServername(servername).
			WithCertificate(certificate).
			WithPkey(pkey).
			WithClientCAs(clientcas)
	}
	return options
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

func printRoot(root *schema.Root) {
	if root.Root == nil {
		fmt.Printf("Immudb is empty\n")
		return
	}
	fmt.Printf(`index:		%d
hash:		%x
`, root.Index, root.Root)
}

func printByIndex(item *schema.StructuredItem) {
	dig, _ := item.Hash()
	fmt.Printf(`index:		%d
key:		%s
value:		%s
hash:		%x
time:		%s
`, item.Index, item.Key, item.Value, dig, time.Unix(int64(item.Value.Timestamp), 0))
}

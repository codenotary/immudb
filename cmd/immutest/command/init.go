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

package immutest

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/codenotary/immudb/pkg/client/tokenservice"

	"github.com/codenotary/immudb/cmd/docs/man"
	c "github.com/codenotary/immudb/cmd/helper"
	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/codenotary/immudb/pkg/auth"
	"github.com/codenotary/immudb/pkg/client"
	"github.com/codenotary/immudb/pkg/server"
	"github.com/jaswdr/faker"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

type commandline struct {
	immuClient client.ImmuClient
	pwr        c.PasswordReader
	tr         c.TerminalReader
	tkns       tokenservice.TokenService
	config     c.Config
	onError    func(err error)
}

const defaultNbEntries = 100

// Init initializes the command
func Init(cmd *cobra.Command, cl *commandline) {
	defaultDb := server.DefaultDBName
	defaultUser := auth.SysAdminUsername
	defaultPassword := auth.SysAdminPassword

	if err := cl.configureFlags(cmd, defaultDb, defaultUser); err != nil {
		cl.onError(err)
		return
	}

	cmd.Use = "immutest [n]"
	cmd.Short = "Populate immudb with the (optional) number of entries (100 by default)"
	cmd.Long = fmt.Sprintf(`Populate immudb with the (optional) number of entries (100 by default).
  Environment variables:
    IMMUTEST_IMMUDB_ADDRESS=127.0.0.1
    IMMUTEST_IMMUDB_PORT=3322
    IMMUTEST_DATABASE=%s
    IMMUTEST_USER=%s
    IMMUTEST_TOKENFILE=token_admin`,
		defaultDb, defaultUser)
	cmd.Example = `  immutest
  immutest 1000
  immutest 500 --database some-database --user some-user`
	cmd.PersistentPreRunE = func(cmd *cobra.Command, args []string) error {
		return cl.config.LoadConfig(cmd)
	}
	cmd.RunE = func(cmd *cobra.Command, args []string) error {
		if err := cl.connect(cmd, nil); err != nil {
			cl.onError(err)
			return err
		}
		defer cl.disconnect(cmd, nil)
		db := viper.GetString("database")
		user := viper.GetString("user")
		ctx := context.Background()
		onSuccess := func() { reconnect(cl, cmd) } // used to redial with new token
		login(ctx, cl, cl.immuClient, cl.pwr, cl.tkns, user, defaultUser, defaultPassword, onSuccess)
		selectDb(ctx, cl, cl.immuClient, cl.tkns, db, onSuccess)
		nbEntries := parseNbEntries(args, cl)
		fmt.Printf("Database %s will be populated with %d entries.\n", db, nbEntries)
		askUserToConfirmOrCancel(cl.tr, cl)
		fmt.Printf("Populating immudb with %d sample entries (credit cards of clients) ...\n", nbEntries)
		took := populate(ctx, cl, &cl.immuClient, nbEntries)
		fmt.Printf(
			"OK: %d entries were written in %v\nNow you can run, for example:\n"+
				"  ./immuclient scan client    to fetch the populated entries\n"+
				"  ./immuclient count client   to count them\n", nbEntries, took)
		return nil
	}
	cmd.Args = cobra.MaximumNArgs(1)
	cmd.DisableAutoGenTag = true
	cmd.AddCommand(man.Generate(cmd, "immutest", "./cmd/docs/man/immutest"))
}

func reconnect(cl *commandline, cmd *cobra.Command) {
	cl.disconnect(cmd, nil)
	if err := cl.connect(cmd, nil); err != nil {
		cl.onError(err)
		return
	}
}

func parseNbEntries(args []string, cl *commandline) int {
	nbEntries := defaultNbEntries
	if len(args) > 0 {
		var err error
		nbEntries, err = strconv.Atoi(args[0])
		if err != nil {
			cl.onError(err)
			return nbEntries
		}
		if nbEntries <= 0 {
			cl.onError(fmt.Errorf(
				"Please specify a number of entries greater than 0 or call the command without "+
					"any argument so that the default number of %d entries will be used", defaultNbEntries))
			return nbEntries
		}
	}
	return nbEntries
}

func login(
	ctx context.Context,
	cl *commandline,
	immuClient client.ImmuClient,
	pwr c.PasswordReader,
	tkns tokenservice.TokenService,
	user string,
	defaultUser string,
	defaultPassword string,
	onSuccess func()) {
	if user == defaultUser {
		_, err := immuClient.Login(ctx, []byte(user), []byte(defaultPassword))
		if err == nil {
			onSuccess()
			return
		}
	}
	pass, err := pwr.Read(fmt.Sprintf("%s's password:", user))
	if err != nil {
		cl.onError(err)
		return
	}
	response, err := immuClient.Login(ctx, []byte(user), pass)
	if err != nil {
		cl.onError(err)
		return
	}
	if err := tkns.SetToken("", response.GetToken()); err != nil {
		cl.onError(err)
		return
	}
	onSuccess()
}

func selectDb(
	ctx context.Context,
	cl *commandline,
	immuClient client.ImmuClient,
	tkns tokenservice.TokenService,
	db string,
	onSuccess func()) {
	_, err := immuClient.UseDatabase(ctx, &schema.Database{DatabaseName: db})
	if err != nil {
		cl.onError(err)
		return
	}
	onSuccess()
}

func askUserToConfirmOrCancel(tr c.TerminalReader, cl *commandline) {
	fmt.Printf("Are you sure you want to proceed? [y/N]: ")
	answer, err := tr.ReadFromTerminalYN("N")
	if err != nil || !(strings.ToUpper("Y") == strings.TrimSpace(strings.ToUpper(answer))) {
		cl.onError(errors.New("Canceled"))
		return
	}
}

func populate(ctx context.Context, cl *commandline, immuClient *client.ImmuClient, nbEntries int) time.Duration {
	// batchSize := 100
	// var keyReaders []io.Reader
	// var valueReaders []io.Reader
	generator := faker.New()
	p := generator.Person()
	py := generator.Payment()
	start := time.Now()
	end := start
	for i := 0; i < nbEntries; i++ {
		var key []byte
		if i%2 == 0 {
			key = []byte(fmt.Sprintf("client:%s %s %s", p.TitleFemale(), p.FirstNameFemale(), p.LastName()))
		} else {
			key = []byte(fmt.Sprintf("client:%s %s %s", p.TitleMale(), p.FirstNameMale(), p.LastName()))
		}
		value := []byte(fmt.Sprintf(
			"card:%s %s %s",
			py.CreditCardType(),
			py.CreditCardNumber(),
			py.CreditCardExpirationDateString()))
		//===> simple Set-based version
		itemStart := time.Now()
		if _, err := (*immuClient).Set(ctx, key, value); err != nil {
			cl.onError(err)
			return 0
		}
		end = end.Add(time.Since(itemStart))
		fmt.Printf("%s = %s\n", key, value)
		//<===
		// FIXME OGG:
		//===> Batch version: seems it doesn't work correctly: get and scan fail afterwards
		// keyReaders = append(keyReaders, bytes.NewReader(key))
		// valueReaders = append(valueReaders, bytes.NewReader(value))
		// if i%batchSize == 0 || i == nbEntries-1 {
		// 	if _, err := (*immuClient).SetBatch(ctx, &client.BatchRequest{
		// 		Keys:   keyReaders,
		// 		Values: valueReaders,
		// 	}); err != nil {
		// 		cl.onError(err)
		//		return 0
		// 	}
		// 	end = end.Add(time.Since(batchStart))
		// 	keyReaders = nil
		// 	valueReaders = nil
		// }
		//<===
	}
	return end.Sub(start)
}

func options() *client.Options {
	port := viper.GetInt("immudb-port")
	address := viper.GetString("immudb-address")
	tokenFileName := viper.GetString("tokenfile")
	if !strings.HasSuffix(tokenFileName, client.AdminTokenFileSuffix) {
		tokenFileName += client.AdminTokenFileSuffix
	}
	options := client.DefaultOptions().
		WithPort(port).
		WithAddress(address).
		WithAuth(true).
		WithTokenFileName(tokenFileName)
	return options
}

func (cl *commandline) disconnect(cmd *cobra.Command, args []string) {
	if err := cl.immuClient.Disconnect(); err != nil {
		cl.onError(err)
		return
	}
}

func (cl *commandline) connect(cmd *cobra.Command, args []string) (err error) {
	if cl.immuClient, err = client.NewImmuClient(options()); err != nil {
		cl.onError(err)
		return
	}
	return
}

func (cl *commandline) configureFlags(
	cmd *cobra.Command,
	defaultDb string,
	defaultUser string,
) error {
	cmd.PersistentFlags().IntP("immudb-port", "p", client.DefaultOptions().Port, "immudb port number")
	cmd.PersistentFlags().StringP("immudb-address", "a", client.DefaultOptions().Address, "immudb host address")
	cmd.PersistentFlags().StringP("database", "d", defaultDb, "database to populate")
	cmd.PersistentFlags().StringP("user", "u", defaultUser, "database user")
	cmd.PersistentFlags().StringVar(&cl.config.CfgFn, "config", "", "config file (default path are configs or $HOME. Default filename is immutest.toml)")

	if err := viper.BindPFlag("immudb-port", cmd.PersistentFlags().Lookup("immudb-port")); err != nil {
		return err
	}
	if err := viper.BindPFlag("immudb-address", cmd.PersistentFlags().Lookup("immudb-address")); err != nil {
		return err
	}
	if err := viper.BindPFlag("database", cmd.PersistentFlags().Lookup("database")); err != nil {
		return err
	}
	if err := viper.BindPFlag("user", cmd.PersistentFlags().Lookup("user")); err != nil {
		return err
	}

	viper.SetDefault("immudb-port", client.DefaultOptions().Port)
	viper.SetDefault("immudb-address", client.DefaultOptions().Address)
	viper.SetDefault("database", defaultDb)
	viper.SetDefault("user", defaultUser)

	return nil
}

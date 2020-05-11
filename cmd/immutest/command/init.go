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

package immutest

import (
	"context"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/codenotary/immudb/cmd/docs/man"
	c "github.com/codenotary/immudb/cmd/helper"
	"github.com/codenotary/immudb/pkg/client"
	"github.com/codenotary/immudb/pkg/gw"
	"github.com/jaswdr/faker"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

type commandline struct {
	immuClient client.ImmuClient
}

const defaultNbEntries = 100

func Init(cmd *cobra.Command, o *c.Options) {
	if err := configureOptions(cmd, o); err != nil {
		c.QuitToStdErr(err)
	}
	cl := new(commandline)

	cmd.Use = "immutest [n]"
	cmd.Short = "Populate immudb with the (optional) number of entries (100 by default)"
	cmd.Long = `Populate immudb with the (optional) number of entries (100 by default).
  Environment variables:
    IMMUTEST_IMMUDB-ADDRESS=127.0.0.1
    IMMUTEST_IMMUDB-PORT=3322`
	cmd.Example = "immutest 1000"
	cmd.RunE = func(cmd *cobra.Command, args []string) error {
		cl.connect(cmd, nil)
		defer cl.disconnect(cmd, nil)
		serverAddress := cl.immuClient.GetOptions().Address
		if serverAddress != "127.0.0.1" && serverAddress != "localhost" {
			c.QuitToStdErr(errors.New(
				"immutest is allowed to run only on the server machine (remote runs are not allowed)"))
		}
		checkForEmptyDB(serverAddress)
		nbEntries := defaultNbEntries
		var err error
		if len(args) > 0 {
			nbEntries, err = strconv.Atoi(args[0])
			if err != nil {
				c.QuitWithUserError(err)
			}
			if nbEntries <= 0 {
				c.QuitWithUserError(fmt.Errorf(
					"Please specify a number of entries greater than 0 or call the command without "+
						"any argument so that the default number of %d entries will be used", defaultNbEntries))
			}
		}
		fmt.Printf("Populating immudb with %d sample entries (credit cards of clients) ...\n", nbEntries)
		took := populate(&cl.immuClient, nbEntries)
		fmt.Printf(
			"OK: %d entries were written in %v\n"+
				"Run './immuclient scan client:' to fetch the populated entries", nbEntries, took)
		return nil
	}
	cmd.Args = cobra.MaximumNArgs(1)
	cmd.DisableAutoGenTag = true
	cmd.AddCommand(man.Generate(cmd, "immutest", "./cmd/docs/man/immutest"))
}

func checkForEmptyDB(serverAddress string) {
	metricsURL := "http://" + serverAddress + ":9497/metrics"
	httpClient := http.Client{Timeout: 3 * time.Second}
	resp, err := httpClient.Get(metricsURL)
	if err != nil {
		fmt.Printf(
			"Error determining if this is a clean run (i.e. if db is empty or not):\n%v",
			err)
		askUserToConfirmOrCancel()
	}
	defer resp.Body.Close()
	body, _ := ioutil.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		fmt.Printf(
			"Error determining if this is a clean run (i.e. if db is empty or not):\n"+
				"GET metrics from URL %s returned unexpected HTTP Status %d with body %s",
			metricsURL,
			resp.StatusCode,
			string(body),
		)
		askUserToConfirmOrCancel()
	}
	var nbMetricsFound int
	var dbSize uint64
	for _, ml := range strings.Split(string(body), "\n") {
		if (strings.Index(ml, "immudb_lsm_size_bytes") == 0 ||
			strings.Index(ml, "immudb_vlog_size_bytes") == 0) &&
			// ignore the size of the immudbsys
			!strings.Contains(ml, "sys") {
			mlPieces := strings.Split(ml, " ")
			if len(mlPieces) > 0 {
				s, err := strconv.ParseUint(mlPieces[len(mlPieces)-1], 10, 64)
				if err == nil {
					dbSize += s
					nbMetricsFound++
				}
			}
		}
	}
	if nbMetricsFound != 2 {
		fmt.Println(
			"Unable to safely determine if this is a clean run (i.e. if db is empty or not)")
		askUserToConfirmOrCancel()
	} else if dbSize > 0 {
		fmt.Println(
			"It looks like this might not be a clean run (i.e. the database might not empty)")
		askUserToConfirmOrCancel()
	}
}

func askUserToConfirmOrCancel() {
	var answer string
	fmt.Printf("Are you sure you want to proceed? [y/N]: ")
	if _, err := fmt.Scanln(&answer); err != nil ||
		!(strings.ToUpper("Y") == strings.TrimSpace(strings.ToUpper(answer))) {
		c.QuitToStdErr("Canceled")
	}
}

func populate(immuClient *client.ImmuClient, nbEntries int) time.Duration {
	// batchSize := 100
	// var keyReaders []io.Reader
	// var valueReaders []io.Reader
	generator := faker.New()
	p := generator.Person()
	py := generator.Payment()
	ctx := context.Background()
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
			c.QuitWithUserError(err)
		}
		end = end.Add(time.Since(itemStart))
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
		// 		c.QuitWithUserError(err)
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
	options := client.DefaultOptions().
		WithPort(port).
		WithAddress(address).
		WithAuth(true)
	return options
}

func (cl *commandline) disconnect(cmd *cobra.Command, args []string) {
	if err := cl.immuClient.Disconnect(); err != nil {
		c.QuitToStdErr(err)
	}
}

func (cl *commandline) connect(cmd *cobra.Command, args []string) (err error) {
	if cl.immuClient, err = client.NewImmuClient(options()); err != nil {
		c.QuitToStdErr(err)
	}
	return
}

func configureOptions(cmd *cobra.Command, o *c.Options) error {
	cmd.PersistentFlags().IntP("immudb-port", "p", gw.DefaultOptions().ImmudbPort, "immudb port number")
	cmd.PersistentFlags().StringP("immudb-address", "a", gw.DefaultOptions().ImmudbAddress, "immudb host address")
	cmd.PersistentFlags().StringVar(&o.CfgFn, "config", "", "config file (default path are configs or $HOME. Default filename is immutest.toml)")
	if err := viper.BindPFlag("immudb-port", cmd.PersistentFlags().Lookup("immudb-port")); err != nil {
		return err
	}
	if err := viper.BindPFlag("immudb-address", cmd.PersistentFlags().Lookup("immudb-address")); err != nil {
		return err
	}
	viper.SetDefault("immudb-port", gw.DefaultOptions().ImmudbPort)
	viper.SetDefault("immudb-address", gw.DefaultOptions().ImmudbAddress)
	return nil
}

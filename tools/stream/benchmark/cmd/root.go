/*
Copyright 2022 CodeNotary, Inc. All rights reserved.

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

package cmd

import (
	"fmt"
	"github.com/spf13/cobra"
	"os"
	"os/user"
	"streamer/benchmark/streamb"

	"github.com/spf13/viper"
)

var cfgFile string

var rootCmd = &cobra.Command{
	Use:   "streamb",
	Short: "Immudb stream benchmark",
	Long:  `Immudb stream benchmark.`,

	RunE: func(cmd *cobra.Command, args []string) error {
		iterations, err := cmd.Flags().GetInt("iterations")
		if err != nil {
			return err
		}
		workers, err := cmd.Flags().GetInt("workers")
		if err != nil {
			return err
		}
		kvLength, err := cmd.Flags().GetInt("kv-length")
		if err != nil {
			return err
		}
		maxValueSize, err := cmd.Flags().GetInt("max-value-size")
		if err != nil {
			return err
		}
		cleanIndex, err := cmd.Flags().GetBool("clean-index")
		if err != nil {
			return err
		}
		cleanIndexFreq, err := cmd.Flags().GetInt("clean-index-freq")
		if err != nil {
			return err
		}
		return streamb.Benchmark(iterations, workers, maxValueSize, kvLength, cleanIndex, cleanIndexFreq)
	},
}

func Execute() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func init() {
	cobra.OnInitialize(initConfig)

	rootCmd.PersistentFlags().StringVar(&cfgFile, "config", "", "config file (default is $HOME/.streamb.yaml)")

	rootCmd.Flags().IntP("iterations", "i", 8, "Number of iterations")
	rootCmd.Flags().IntP("workers", "w", 4, "Number of workers")
	rootCmd.Flags().IntP("kv-length", "l", 3, "Max key values length")
	rootCmd.Flags().IntP("max-value-size", "s", 25_000_000, "Max value size")
	rootCmd.Flags().Bool("clean-index", true, "Clean index during execution")
	rootCmd.Flags().Int("clean-index-freq", 60, "Clean index frequency. Seconds")
}

func initConfig() {
	if cfgFile != "" {
		// Use config file from the flag.
		viper.SetConfigFile(cfgFile)
	} else {
		// Find home directory.
		user, err := user.Current()
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}

		// Search config in home directory with name ".streamb" (without extension).
		viper.AddConfigPath(user.HomeDir)
		viper.SetConfigName(".streamb")
	}

	viper.AutomaticEnv() // read in environment variables that match

	// If a config file is found, read it in.
	if err := viper.ReadInConfig(); err == nil {
		fmt.Println("Using config file:", viper.ConfigFileUsed())
	}
}

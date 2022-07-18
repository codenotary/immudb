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
package main

import (
	"crypto/rand"
	"encoding/binary"
	"encoding/json"
	"flag"
	"log"
	"os"
	"time"

	"github.com/codenotary/immudb/test/performance-test-suite/pkg/runner"
)

func main() {

	flDuration := flag.Duration("d", time.Second*10, "duration of each test run")
	flSeed := flag.Uint64("s", 0, "seed for data generators")
	flRandomSeed := flag.Bool("random-seed", false, "if set to true, use random seed for test runs")

	flag.Parse()

	if *flRandomSeed {
		var rndSeed [8]byte
		_, err := rand.Reader.Read(rndSeed[:])
		if err != nil {
			log.Fatalf("Couldn't initialize random seed: %v", err)
		}
		*flSeed = binary.BigEndian.Uint64(rndSeed[:])
	}

	results, err := runner.RunAllBenchmarks(*flDuration, *flSeed)
	if err != nil {
		log.Fatal(err)
	}

	e := json.NewEncoder(os.Stdout)
	e.SetIndent("", "   ")
	err = e.Encode(results)
	if err != nil {
		log.Fatal(err)
	}
}

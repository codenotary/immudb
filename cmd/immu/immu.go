/*
Copyright 2019 vChain, Inc.

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
	"fmt"
	"os"

	"github.com/codenotary/immudb/pkg/client"
)

func main() {
	if len(os.Args) < 2 {
		usage()
	} else if os.Args[1] == "get" {
		get()
	} else if os.Args[1] == "set" {
		set()
	} else {
		usage()
	}
}

func usage() {
	fmt.Println("Usage:", os.Args[0], "<set|get> key (value)?")
	os.Exit(1)
}

func set() {
	if len(os.Args) < 4 {
		usage()
	}
	key, value := os.Args[2], os.Args[3]
	if err := client.Set(client.DefaultOptions(), key, value); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	fmt.Println("Set", key, "=", value)
}

func get() {
	if len(os.Args) < 3 {
		usage()
	}
	key := os.Args[2]
	response, err := client.Get(client.DefaultOptions(), key)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	fmt.Println("Get", key, "=", string(response))
}

# Autotest

These tests simulates interactive use of the tools.

To launch the tests, simply invoke `make` in this directory.

## Requirements

In order to execute the test, you need the "expect" cli tool, which should be available from your distro's repository:

 - To install `expect` on debian/ubuntu, just `apt-get install expect`
 - To install `expect` on RedHat/CentOS, `yum install expect`

 You'll also need the immu* tools, which should be already built in the main directory.

## autotest.sh

This script starts immudb, logs in, changes the admin password, create a bunch of databases and populates them with deterministic values. It next proceed to read them back, via index and via key, checking the correctness of the answer.

## envpasswd.sh

This scripts checks if the env variable IMMUDB_ADMIN_PASSWORD is working as it should be.

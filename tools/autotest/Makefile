SHELL=/bin/bash -o pipefail

.PHONY: all
all: requirements test1 test2

.PHONY: requirements
requirements:
	@expect -v >/dev/null 2>&1 || (echo "Please install 'expect'"; return 1)

.PHONY: test1
test1:
	./autotest.sh

.PHONY: test2
test2:
	./envpasswd.sh

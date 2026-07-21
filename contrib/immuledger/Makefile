.PHONY: build test vet clean

build:
	mkdir -p bin
	go build -o bin/immuledger ./cmd/immuledger

test:
	go test ./...

vet:
	go vet ./...

clean:
	rm -rf bin

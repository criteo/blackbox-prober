.PHONY: test build build_linux

test:
		go test ./...

build:
		go build -o build/aerospike_probe probes/aerospike/*.go

build_linux:
		GOOS=linux GOARCH=amd64 go build -o build/aerospike_probe probes/aerospike/*.go

IMG ?= blackbox-prober:latest

.PHONY: test build build_linux lint

test:
		go test ./...

build:
		go build -o build/aerospike_probe probes/aerospike/main.go

build_linux:
		GOOS=linux GOARCH=amd64 go build -o build/aerospike_probe probes/aerospike/*.go

lint:
		gofmt -d -e -s pkg/**/*.go probes/**/*.go
		go vet ./...

.PHONY: image
image:
	docker build -t ${IMG} .
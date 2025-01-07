IMG ?= blackbox-prober:latest

.PHONY: test build build_linux lint

test:
		go test ./...

build:
		CGO_ENABLED=0 go build -o build/aerospike_probe probes/aerospike/main.go

build_linux:
		CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o build/aerospike_probe probes/aerospike/*.go

lint:
		gofmt -d -e -s pkg/**/*.go probes/**/*.go
		go vet ./...

.PHONY: image
image:
	docker build -t ${IMG} .
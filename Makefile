IMG ?= blackbox-prober:latest

.PHONY: test build_linux build_aerospike build_linux_aerospike build_milvus build_linux_milvus lint image

test:
		go test ./...

build_aerospike:
		CGO_ENABLED=0 go build -o build/aerospike_probe probes/aerospike/main.go

build_linux_aerospike:
		CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o build/aerospike_probe probes/aerospike/*.go

build_milvus:
		CGO_ENABLED=0 go build -o build/milvus_probe probes/milvus/main.go

build_linux_milvus:
		CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o build/milvus_probe probes/milvus/*.go

build_linux: build_linux_aerospike build_linux_milvus

lint:
		gofmt -d -e -s pkg/**/*.go probes/**/*.go
		go vet ./...

image:
	docker build -t ${IMG} .
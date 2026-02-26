IMG ?= blackbox-prober:latest

.PHONY: test build_linux build_aerospike build_linux_aerospike build_milvus build_linux_milvus lint image

test:
		go test ./...

build: build_aerospike build_milvus build_opensearch

build_aerospike:
		CGO_ENABLED=0 go build -o build/aerospike_probe ./cmd/aerospike

build_linux_aerospike:
		CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o build/aerospike_probe ./cmd/aerospike

build_milvus:
		CGO_ENABLED=0 go build -o build/milvus_probe ./cmd/milvus

build_linux_milvus:
		CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o build/milvus_probe ./cmd/milvus

build_opensearch:
		CGO_ENABLED=0 go build -o build/opensearch_probe ./cmd/opensearch

build_linux_opensearch:
		CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o build/opensearch_probe ./cmd/opensearch

build_linux: build_linux_aerospike build_linux_milvus build_linux_opensearch

lint:
		gofmt -d -e -s pkg/**/*.go cmd/**/*.go
		go vet ./...

image:
	docker build -t ${IMG} .
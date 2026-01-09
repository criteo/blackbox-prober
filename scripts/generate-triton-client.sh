#!/bin/bash
set -euo pipefail

# Generate Triton gRPC client code from protobuf definitions 
#    - clones the Triton common and client repos,
#    - generates Go code
#    - commits the resulted package in the pkg/triton directory.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(dirname "$SCRIPT_DIR")"
TMP_DIR=$(mktemp -d)
PACKAGE_NAME="client"
OUTPUT_DIR="$REPO_ROOT/pkg/triton/$PACKAGE_NAME"

TRITON_COMMON_REPO="https://github.com/triton-inference-server/common.git"
TRITON_CLIENT_REPO="https://github.com/triton-inference-server/client.git"

cleanup() {
    echo "Cleaning up temporary directory: $TMP_DIR"
    rm -rf "$TMP_DIR"
}

trap cleanup EXIT

echo "=== Triton gRPC Client Generator ==="
echo "Output directory: $OUTPUT_DIR"
echo ""

# Create output directory
mkdir -p "$OUTPUT_DIR"

# Clone repos (shallow, faster)
echo "Cloning Triton repositories..."
git clone --depth 1 "$TRITON_COMMON_REPO" "$TMP_DIR/common"
git clone --depth 1 "$TRITON_CLIENT_REPO" "$TMP_DIR/triton-client-repo"

# Generate Go code from proto files
if ! command -v protoc &> /dev/null; then
    echo "Error: protoc not found. Please install protoc first."
    echo "  macOS: brew install protobuf"
    echo "  Linux: apt-get install protobuf-compiler"
    exit 1
fi

if ! command -v protoc-gen-go &> /dev/null; then
    echo "Installing protoc-gen-go..."
    go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
    exit 1
fi
if ! command -v protoc-gen-go-grpc &> /dev/null; then
    echo "Installing protoc-gen-go-grpc..."
    go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
fi

GEN_SCRIPT="$TMP_DIR/triton-client-repo/src/grpc_generated/go/gen_go_stubs.sh"

mv "$GEN_SCRIPT" "$TMP_DIR/gen_go_stubs.sh"

cd "$TMP_DIR"
chmod +x gen_go_stubs.sh

cat gen_go_stubs.sh
# Patch to ensure PACKAGE="triton-client" in gen_go_stubs.sh before running
sed -i.bak "s/^PACKAGE=".*"/PACKAGE="$PACKAGE_NAME"/" gen_go_stubs.sh

./gen_go_stubs.sh
mv "$TMP_DIR/$PACKAGE_NAME"/* "$OUTPUT_DIR/"

echo "✓ Generated files in: $OUTPUT_DIR"
# find "$OUTPUT_DIR" -name "*.pb.go" | sed "s|$REPO_ROOT/||" | sort
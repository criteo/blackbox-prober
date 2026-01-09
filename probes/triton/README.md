# Triton Probe

Blackbox probe for [NVIDIA Triton Inference Server](https://github.com/triton-inference-server/server). Monitors health, latency, and model availability via gRPC.

## Refreshing the Triton gRPC Client

The Triton gRPC client is auto-generated from upstream protobuf definitions. To update after a new Triton release:

```bash
make refresh-triton-client # To be run from the root of the repository
```

This clones :

- [`triton-inference-server/common`](https://github.com/triton-inference-server/common) : contains the protobuf definitions
- [`triton-inference-server/client`](https://github.com/triton-inference-server/client) : contains the client utilities

Then runs `protoc` and outputs generated client to `pkg/triton/client/`.

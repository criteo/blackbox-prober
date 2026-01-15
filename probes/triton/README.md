# Triton Probe

Blackbox probe for [NVIDIA Triton Inference Server](https://github.com/triton-inference-server/server). Monitors inference latency across all models via gRPC.

# TODOs

- [ ] Add configurable metric labels from Consul `ServiceEntry.Meta` / `Node.Meta`
- [ ] Make `RepositoryIndex` async (can take ~4s, currently blocks Refresh)
- [ ] Support models expecting image inputs (BYTES with shape [1] containing JPEG/PNG) - those are skipped for now
- [ ] Support models expecting JSON dict inputs (Python models with custom preprocessing) - those are skipped for now

## Configuration

See [`configs/triton/triton_config.yaml`](../../configs/triton/triton_config.yaml) for a detailed example.

## Architecture

**One probe per Triton instance**: Each server discovered via Consul becomes a separate `TritonEndpoint`. The probe connects to each instance independently and runs checks against all models hosted on that server.

**Node checks only**: Cluster-level checks are disabled—each Triton server is probed individually, which provides per-instance visibility.

## LatencyCheck

The latency check:

1. Iterates over all **ready models** on the Triton instance
2. Builds a synthetic inference request with random tensor data
3. Executes gRPC `ModelInfer` and records latency

**Model discovery is dynamic**: The `Refresh()` loop (every 30s by default) calls `RepositoryIndex` to discover new/removed models. Checks always use the latest model list.

### Random Tensor Generation

The probe auto-generates valid inference requests by reading model metadata and building random input tensors:

- **Supported data types**: `BOOL`, `INT8/16/32/64`, `UINT8/16/32/64`, `FP16/32/64`, `BYTES` (strings)
- **Sequence batching**: Automatically adds `sequence_id`/`sequence_start` params for sequence models

## Refreshing the Triton gRPC Client

The gRPC client is auto-generated from upstream protobuf definitions:

```bash
make refresh-triton-client  # Run from repo root
```

This clones :

- [`triton-inference-server/common`](https://github.com/triton-inference-server/common) : contains the protobuf definitions
- [`triton-inference-server/client`](https://github.com/triton-inference-server/client) : contains the client utilities

Then runs `protoc` and outputs generated client to `pkg/triton/client/`.

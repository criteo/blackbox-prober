package triton

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/criteo/blackbox-prober/pkg/discovery"
	"github.com/criteo/blackbox-prober/pkg/triton/client"
	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// ModelInfo holds cached metadata and configuration for a single model.
type ModelInfo struct {
	Name     string
	Version  string
	Metadata *client.ModelMetadataResponse
	Config   *client.ModelConfig
}

// TritonEndpoint represents a single Triton Inference Server instance.
// Each server in a cluster (Consul service) is a separate endpoint.
type TritonEndpoint struct {
	Name        string
	ClusterName string
	Address     string // host:port for gRPC
	PodName     string // k8s pod name (from discovery metadata)

	conn       *grpc.ClientConn
	grpcClient client.GRPCInferenceServiceClient
	generator  *Generator

	// Cached metadata for ALL models (auto-discovered)
	models   map[string]*ModelInfo
	modelsMu sync.RWMutex

	Logger log.Logger
	Config *TritonEndpointConfig
}

// NewTritonEndpoint creates a TritonEndpoint from a Consul service entry.
func NewTritonEndpoint(logger log.Logger, clusterName string, entry discovery.ServiceEntry, config *TritonEndpointConfig) *TritonEndpoint {
	return &TritonEndpoint{
		Name:        fmt.Sprintf("%s-%s", entry.Service, entry.Address),
		ClusterName: clusterName,
		Address:     fmt.Sprintf("%s:%d", entry.Address, entry.Port),
		PodName:     entry.Meta["k8s_pod"],
		Logger:      log.With(logger, "endpoint", entry.Address, "cluster", clusterName),
		Config:      config,
	}
}

func (e *TritonEndpoint) GetHash() string {
	return fmt.Sprintf("%s/%s", e.ClusterName, e.Address)
}

// GetName returns a display name for this endpoint.
func (e *TritonEndpoint) GetName() string {
	return e.Name
}

// IsCluster returns false since each TritonEndpoint represents a single server instance,
// not the entire cluster.
func (e *TritonEndpoint) IsCluster() bool {
	return false
}

// Connect establishes a gRPC connection to the Triton server,
// verifies the server is ready, and performs initial model discovery.
func (e *TritonEndpoint) Connect() error {
	ctx, cancel := e.contextWithTimeout()
	defer cancel()

	// Dial gRPC connection
	opts := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	}
	conn, err := grpc.DialContext(ctx, e.Address, opts...)
	if err != nil {
		return fmt.Errorf("failed to dial triton server at %s: %w", e.Address, err)
	}
	e.conn = conn
	e.grpcClient = client.NewGRPCInferenceServiceClient(conn)
	e.generator = NewGenerator()
	e.models = make(map[string]*ModelInfo)

	// Verify server is ready
	readyResp, err := e.grpcClient.ServerReady(ctx, &client.ServerReadyRequest{})
	if err != nil {
		e.conn.Close()
		return fmt.Errorf("failed to check server readiness: %w", err)
	}
	if !readyResp.GetReady() {
		e.conn.Close()
		return fmt.Errorf("triton server at %s is not ready", e.Address)
	}

	level.Info(e.Logger).Log("msg", "Connected to Triton server", "address", e.Address)

	// Perform initial model discovery
	// RepositoryIndex can take a long time to complete, to at some point we should make it async if blocking the main thread is a problem
	err = e.Refresh()
	if err != nil {
		return fmt.Errorf("failed to refresh model cache: %w", err)
	}
	return nil
}

// Refresh discovers all ready models and caches their metadata and configuration.
// Called periodically by the scheduler.
func (e *TritonEndpoint) Refresh() error {
	ctx, cancel := e.contextWithTimeout()
	defer cancel()

	// Get list of all ready models
	indexResp, err := e.grpcClient.RepositoryIndex(ctx, &client.RepositoryIndexRequest{
		Ready: true, // Only get models that are ready for inference
	})
	if err != nil {
		return fmt.Errorf("failed to get repository index: %w", err)
	}

	newModels := make(map[string]*ModelInfo)
	for _, modelIndex := range indexResp.GetModels() {
		modelName := modelIndex.GetName()
		modelVersion := modelIndex.GetVersion()

		// Use model name + version as key for uniqueness
		modelKey := modelName
		if modelVersion != "" {
			modelKey = fmt.Sprintf("%s:%s", modelName, modelVersion)
		}

		// Fetch model metadata
		metadata, err := e.grpcClient.ModelMetadata(ctx, &client.ModelMetadataRequest{
			Name:    modelName,
			Version: modelVersion,
		})
		if err != nil {
			level.Warn(e.Logger).Log(
				"msg", "Failed to get model metadata",
				"model", modelName,
				"version", modelVersion,
				"err", err,
			)
			continue
		}
		// Fetch model configuration
		configResp, err := e.grpcClient.ModelConfig(ctx, &client.ModelConfigRequest{
			Name:    modelName,
			Version: modelVersion,
		})
		if err != nil {
			level.Warn(e.Logger).Log(
				"msg", "Failed to get model config",
				"model", modelName,
				"version", modelVersion,
				"err", err,
			)
			continue
		}

		newModels[modelKey] = &ModelInfo{
			Name:     modelName,
			Version:  modelVersion,
			Metadata: metadata,
			Config:   configResp.GetConfig(),
		}
	}

	// Update cached models atomically
	e.modelsMu.Lock()
	e.models = newModels
	e.modelsMu.Unlock()

	level.Debug(e.Logger).Log("msg", "Refreshed model cache", "model_count", len(newModels))
	return nil
}

// Close terminates the gRPC connection to the Triton server.
func (e *TritonEndpoint) Close() error {
	if e.conn != nil {
		if err := e.conn.Close(); err != nil {
			return fmt.Errorf("failed to close grpc connection: %w", err)
		}
		e.conn = nil
		e.grpcClient = nil
	}
	return nil
}

// GetModels returns a copy of the cached model information.
// Safe for concurrent access.
func (e *TritonEndpoint) GetModels() map[string]*ModelInfo {
	e.modelsMu.RLock()
	defer e.modelsMu.RUnlock()

	result := make(map[string]*ModelInfo, len(e.models))
	for k, v := range e.models {
		result[k] = v
	}
	return result
}

// Infer performs an inference request against a model and returns the response.
// This is a reusable building block for various checks.
func (e *TritonEndpoint) Infer(modelInfo *ModelInfo, batchSize int64) (*client.ModelInferResponse, error) {
	request, err := e.generator.BuildInferRequest(
		modelInfo.Metadata,
		modelInfo.Config,
		modelInfo.Name,
		modelInfo.Version,
		batchSize,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to build infer request for model %s: %w", modelInfo.Name, err)
	}

	ctx, cancel := e.contextWithTimeout()
	defer cancel()

	resp, err := e.grpcClient.ModelInfer(ctx, request)
	if err != nil {
		return nil, fmt.Errorf("inference failed for model %s: %w", modelInfo.Name, err)
	}

	return resp, nil
}

// GetGRPCClient returns the gRPC client for making inference requests.
//func (e *TritonEndpoint) GetGRPCClient() client.GRPCInferenceServiceClient {
//	return e.grpcClient
//}
// GetGenerator returns the Generator for building inference requests.
//func (e *TritonEndpoint) GetGenerator() *Generator {
//	return e.generator

// contextWithTimeout returns a context with the configured timeout.
func (e *TritonEndpoint) contextWithTimeout() (context.Context, context.CancelFunc) {
	timeout := 30 * time.Second
	if e.Config != nil && e.Config.Timeout > 0 {
		timeout = e.Config.Timeout
	}
	return context.WithTimeout(context.Background(), timeout)
}

// Package triton provides utilities for interacting with Triton Inference Server.
package triton

import (
	"encoding/binary"
	"fmt"
	"math/rand"
	"time"

	"github.com/criteo/blackbox-prober/pkg/triton/client"
)

// dataTypeByteSizes maps Triton data type names to their byte sizes.
var dataTypeByteSizes = map[string]int64{
	"BOOL":   1,
	"INT8":   1,
	"INT16":  2,
	"INT32":  4,
	"INT64":  8,
	"UINT8":  1,
	"UINT16": 2,
	"UINT32": 4,
	"UINT64": 8,
	"FP16":   2,
	"FP32":   4,
	"FP64":   8,
}

// Generator generates random tensor data for Triton inference requests.
type Generator struct {
	rng *rand.Rand
}

// NewGenerator creates a new Generator with a time-based random seed.
func NewGenerator() *Generator {
	return &Generator{
		rng: rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

// GenerateTensorData generates random bytes for a single tensor element based on metadata.
// It returns the raw bytes representing one element (excluding batch dimension).
func (g *Generator) GenerateTensorData(metadata *client.ModelMetadataResponse_TensorMetadata) ([]byte, error) {
	datatype := metadata.GetDatatype()
	shape := metadata.GetShape()

	if len(shape) < 1 {
		return nil, fmt.Errorf("invalid shape: must have at least 1 dimension, got %d", len(shape))
	}

	// Calculate total elements (excluding batch dimension at index 0 if it's -1)
	totalElements := int64(1)
	startIdx := 0
	if shape[0] == -1 {
		startIdx = 1 // Skip batch dimension
	}
	for i := startIdx; i < len(shape); i++ {
		if shape[i] <= 0 {
			return nil, fmt.Errorf("invalid shape dimension at index %d: %d", i, shape[i])
		}
		totalElements *= shape[i]
	}

	// Handle BYTES (string) datatype specially - Triton requires 4-byte length prefix per string
	if datatype == "BYTES" {
		return g.generateStringData(totalElements), nil
	}

	elementSize, ok := dataTypeByteSizes[datatype]
	if !ok {
		return nil, fmt.Errorf("unsupported datatype: %s", datatype)
	}

	totalBytes := totalElements * elementSize
	return g.randomBytes(totalBytes), nil
}

// randomBytes generates a slice of random bytes of the specified size.
func (g *Generator) randomBytes(size int64) []byte {
	data := make([]byte, size)
	// Read random bytes directly - more efficient than byte-by-byte
	g.rng.Read(data)
	return data
}

// generateStringData generates properly serialized string data for Triton BYTES datatype.
// Triton string format: 4-byte little-endian uint32 length prefix + string bytes per element.
// Generates numeric strings since many models expect parseable numbers
func (g *Generator) generateStringData(numStrings int64) []byte {
	var result []byte
	for i := int64(0); i < numStrings; i++ {
		// Generate a numeric string (many models expect string inputs to be parseable as numbers)
		numValue := g.rng.Int63n(1000000) // Random number 0-999999
		strBytes := []byte(fmt.Sprintf("%d", numValue))
		strLen := len(strBytes)

		// Write 4-byte little-endian length prefix
		lenPrefix := make([]byte, 4)
		binary.LittleEndian.PutUint32(lenPrefix, uint32(strLen))

		result = append(result, lenPrefix...)
		result = append(result, strBytes...)
	}
	return result
}

// BuildInferRequest constructs a ModelInferRequest from model metadata and config.
// It generates random input tensors and handles sequence batching if required.
func (g *Generator) BuildInferRequest(
	metadata *client.ModelMetadataResponse,
	modelConfig *client.ModelConfig,
	modelName string,
	modelVersion string,
	batchSize int64,
) (*client.ModelInferRequest, error) {
	inputs := metadata.GetInputs()
	inferInputs := make([]*client.ModelInferRequest_InferInputTensor, 0, len(inputs))
	rawInputContents := make([][]byte, 0, len(inputs))

	for _, inputMeta := range inputs {
		actualShape := g.resolveShape(inputMeta.GetShape(), batchSize)

		inferInputs = append(inferInputs, &client.ModelInferRequest_InferInputTensor{
			Name:     inputMeta.GetName(),
			Datatype: inputMeta.GetDatatype(),
			Shape:    actualShape,
		})

		// Generate random data for each element in the batch
		rawData, err := g.generateBatchData(inputMeta, batchSize)
		if err != nil {
			return nil, fmt.Errorf("generating data for input %q: %w", inputMeta.GetName(), err)
		}
		rawInputContents = append(rawInputContents, rawData)
	}

	// Build requested outputs
	outputs := metadata.GetOutputs()
	inferOutputs := make([]*client.ModelInferRequest_InferRequestedOutputTensor, 0, len(outputs))
	for _, outputMeta := range outputs {
		inferOutputs = append(inferOutputs, &client.ModelInferRequest_InferRequestedOutputTensor{
			Name: outputMeta.GetName(),
		})
	}

	request := &client.ModelInferRequest{
		ModelName:        modelName,
		ModelVersion:     modelVersion,
		Inputs:           inferInputs,
		Outputs:          inferOutputs,
		RawInputContents: rawInputContents,
	}

	// Add sequence batching parameters if required
	if modelConfig != nil && modelConfig.GetSequenceBatching() != nil {
		g.addSequenceParams(request)
	}

	return request, nil
}

// resolveShape replaces -1 (dynamic batch dimension) with the actual batch size.
func (g *Generator) resolveShape(shape []int64, batchSize int64) []int64 {
	resolved := make([]int64, len(shape))
	for i, dim := range shape {
		if dim == -1 {
			resolved[i] = batchSize
		} else {
			resolved[i] = dim
		}
	}
	return resolved
}

// generateBatchData generates random data for a full batch of a single input tensor.
func (g *Generator) generateBatchData(metadata *client.ModelMetadataResponse_TensorMetadata, batchSize int64) ([]byte, error) {
	var rawData []byte
	for i := int64(0); i < batchSize; i++ {
		element, err := g.GenerateTensorData(metadata)
		if err != nil {
			return nil, err
		}
		rawData = append(rawData, element...)
	}
	return rawData, nil
}

// addSequenceParams adds sequence batching parameters to the request.
func (g *Generator) addSequenceParams(request *client.ModelInferRequest) {
	if request.Parameters == nil {
		request.Parameters = make(map[string]*client.InferParameter)
	}

	// Use timestamp-based correlation ID for uniqueness
	sequenceID := time.Now().UnixNano()
	request.Parameters["sequence_id"] = &client.InferParameter{
		ParameterChoice: &client.InferParameter_Int64Param{
			Int64Param: sequenceID,
		},
	}
	request.Parameters["sequence_start"] = &client.InferParameter{
		ParameterChoice: &client.InferParameter_BoolParam{
			BoolParam: true,
		},
	}
}

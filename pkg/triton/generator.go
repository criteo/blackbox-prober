// Package triton provides utilities for interacting with Triton Inference Server.
package triton

import (
	"encoding/binary"
	"fmt"
	"math"
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

	// Generate type-appropriate data
	return g.generateTypedData(datatype, totalElements)
}

// generateTypedData routes to the appropriate generator for each data type.
func (g *Generator) generateTypedData(datatype string, count int64) ([]byte, error) {
	size, ok := dataTypeByteSizes[datatype]
	if !ok && datatype != "BYTES" {
		return nil, fmt.Errorf("unsupported datatype: %s", datatype)
	}

	switch datatype {
	case "BYTES":
		return g.randomStrings(count), nil
	case "FP32":
		return g.randomFloat32s(count), nil
	case "FP64":
		return g.randomFloat64s(count), nil
	case "INT8", "INT16", "INT32", "INT64", "UINT8", "UINT16", "UINT32", "UINT64":
		return g.randomSmallInts(count, int(size)), nil
	default:
		// BOOL, FP16: random bytes
		return g.randomBytes(count * size), nil
	}
}

// randomBytes fills a slice with random bytes.
func (g *Generator) randomBytes(n int64) []byte {
	data := make([]byte, n)
	g.rng.Read(data)
	return data
}

// randomSmallInts generates integers in [0, 255] to avoid embedding index errors.
// Values are stored little-endian with only the first byte set.
func (g *Generator) randomSmallInts(count int64, byteSize int) []byte {
	data := make([]byte, count*int64(byteSize))
	for i := int64(0); i < count; i++ {
		data[i*int64(byteSize)] = byte(g.rng.Intn(256))
	}
	return data
}

// randomFloat32s generates FP32 floats in [0.0, 1.0).
func (g *Generator) randomFloat32s(count int64) []byte {
	data := make([]byte, count*4)
	for i := int64(0); i < count; i++ {
		binary.LittleEndian.PutUint32(data[i*4:], math.Float32bits(g.rng.Float32()))
	}
	return data
}

// randomFloat64s generates FP64 floats in [0.0, 1.0).
func (g *Generator) randomFloat64s(count int64) []byte {
	data := make([]byte, count*8)
	for i := int64(0); i < count; i++ {
		binary.LittleEndian.PutUint64(data[i*8:], math.Float64bits(g.rng.Float64()))
	}
	return data
}

// randomStrings generates Triton BYTES format: 4-byte length prefix + string content.
// Uses numeric strings since many models expect parseable numbers.
func (g *Generator) randomStrings(count int64) []byte {
	var result []byte
	for i := int64(0); i < count; i++ {
		str := []byte(fmt.Sprintf("%d", g.rng.Intn(1000000)))
		lenPrefix := make([]byte, 4)
		binary.LittleEndian.PutUint32(lenPrefix, uint32(len(str)))
		result = append(result, lenPrefix...)
		result = append(result, str...)
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

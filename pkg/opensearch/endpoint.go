package opensearch

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/go-kit/log"
	"github.com/opensearch-project/opensearch-go/v4/opensearchapi"
	"github.com/opensearch-project/opensearch-go/v4/opensearchutil"
)

type OpenSearchEndpoint struct {
	Name         string
	ClusterLevel bool
	ClusterName  string
	PodName      string
	NodeFqdn     string
	Client       *opensearchapi.Client
	ClientConfig opensearchapi.Config
	Config       OpenSearchEndpointConfig
	Logger       log.Logger
}

func (e *OpenSearchEndpoint) GetHash() string {
	return fmt.Sprintf("%s/%s", e.ClusterName, e.Name)
}

func (e *OpenSearchEndpoint) GetName() string {
	return e.Name
}

func (e *OpenSearchEndpoint) IsCluster() bool {
	return e.ClusterLevel
}

func (e *OpenSearchEndpoint) Connect() error {
	client, err := opensearchapi.NewClient(e.ClientConfig)
	if err != nil {
		return fmt.Errorf("error creating opensearch client: %v", err)
	}
	e.Client = client
	return nil
}

func (e *OpenSearchEndpoint) Refresh() error {
	return nil
}

// There is no Close method for opensearch client
func (e *OpenSearchEndpoint) Close() error {
	return nil
}

// checkIndexExists checks if the specified index exists in OpenSearch.
// It returns true if the index exists, false if it does not, and an error if there was an issue during the check.
func (e *OpenSearchEndpoint) checkIndexExists(indexName string) (bool, error) {
	if e == nil || e.Client == nil {
		return false, fmt.Errorf("opensearch endpoint client not initialized")
	}

	ctx := context.Background()
	response, err := e.Client.Indices.Exists(ctx, opensearchapi.IndicesExistsReq{
		Indices: []string{indexName},
	})

	if err != nil {
		// If a response is present, inspect it safely
		if response != nil {
			if response.StatusCode == 404 {
				return false, nil
			}
			return false, fmt.Errorf("error checking if index %s exists: %v (status:%d)", indexName, err, response.StatusCode)
		}
		return false, fmt.Errorf("error checking if index %s exists: %v", indexName, err)
	}

	if response == nil {
		return false, fmt.Errorf("nil response when checking if index %s exists", indexName)
	}
	defer response.Body.Close()

	switch response.StatusCode {
	case 200:
		return true, nil
	case 404:
		return false, nil
	default:
		return false, fmt.Errorf("unexpected status code %d when checking if index %s exists", response.StatusCode, indexName)
	}
}

// createIndex creates a new index in OpenSearch with the specified name, number of shards, and number of replicas.
func (e *OpenSearchEndpoint) createIndex(indexName string, numberOfShards int, numberOfReplicas int) error {
	ctx := context.Background()

	bodyMap := map[string]interface{}{
		"settings": map[string]interface{}{
			"number_of_shards":   numberOfShards,
			"number_of_replicas": numberOfReplicas,
		},
	}

	bodyBytes, err := json.Marshal(bodyMap)
	if err != nil {
		return fmt.Errorf("error marshaling request body: %v", err)
	}

	response, err := e.Client.Indices.Create(ctx, opensearchapi.IndicesCreateReq{
		Index: indexName,
		Body:  bytes.NewReader(bodyBytes),
	})
	if err != nil {
		return fmt.Errorf("error creating index %s: %v", indexName, err)
	}

	statusCode := response.Inspect().Response.StatusCode

	// Check for successful creation (200 OK or 201 Created)
	if statusCode != 200 && statusCode != 201 {
		return fmt.Errorf("unexpected status code %d when creating index %s", statusCode, indexName)
	}

	return nil
}

// getIndexHealth retrieves the health status of the specified index in OpenSearch.
func (e *OpenSearchEndpoint) getIndexHealth(indexName string) (float64, error) {
	ctx := context.Background()

	// Use the Cat Indices API to get health status for the index
	response, err := e.Client.Cat.Indices(ctx, &opensearchapi.CatIndicesReq{
		Indices: []string{indexName},
	})
	if err != nil {
		return 0, fmt.Errorf("error getting index health for %s: %v", indexName, err)
	}
	defer response.Inspect().Response.Body.Close()

	if response.Inspect().Response.StatusCode != 200 {
		return 0, fmt.Errorf("unexpected status code %d when getting index health for %s", response.Inspect().Response.StatusCode, indexName)
	}

	var indicesInfo []map[string]interface{}
	if err := json.NewDecoder(response.Inspect().Response.Body).Decode(&indicesInfo); err != nil {
		return 0, fmt.Errorf("error decoding index health response: %v", err)
	}
	if len(indicesInfo) == 0 {
		return 0, fmt.Errorf("no health info found for index %s", indexName)
	}

	health, _ := indicesInfo[0]["health"].(string)

	var healthValue float64
	switch health {
	case "green":
		healthValue = 0
	case "yellow":
		healthValue = 1
	case "red":
		healthValue = 2
	default:
		return 0, fmt.Errorf("unknown index health status '%s' for %s", health, e.Name)
	}

	return healthValue, nil
}

func (e *OpenSearchEndpoint) insertDocument(indexName string, documentID string, documentContent string) error {
	ctx := context.Background()
	doc := map[string]interface{}{
		"content": documentContent,
	}

	jsonBody, err := json.Marshal(doc)
	if err != nil {
		return fmt.Errorf("error marshaling document: %v", err)
	}

	response, err := e.Client.Document.Create(ctx, opensearchapi.DocumentCreateReq{
		Index:      indexName,
		DocumentID: documentID,
		Body:       bytes.NewReader(jsonBody),
	})
	if err != nil {
		return fmt.Errorf("error inserting document into index %s: %v", indexName, err)
	}

	if response.Inspect().Response.StatusCode != 201 {
		return fmt.Errorf("unexpected status code %d when inserting document into index %s", response.Inspect().Response.StatusCode, indexName)
	}

	return nil
}

func (e *OpenSearchEndpoint) insertDocumentBulk(indexName string, documentCount int, documentIDPrefix string, documentContent string) error {
	ctx := context.Background()
	indexer, err := opensearchutil.NewBulkIndexer(opensearchutil.BulkIndexerConfig{
		Client:        e.Client,
		Index:         indexName,
		NumWorkers:    4,               // tune based on CPU & cluster
		FlushBytes:    5 << 20,         // ~5MB
		FlushInterval: 5 * time.Second, // time-based flush
	})
	if err != nil {
		return fmt.Errorf("new bulk indexer: %w", err)
	}

	for i := 0; i < documentCount; i++ {
		documentID := fmt.Sprintf("%s-%d", documentIDPrefix, i+1)
		doc := map[string]interface{}{
			"content": documentContent,
			"ID":      documentID,
		}

		var buf bytes.Buffer
		if err := json.NewEncoder(&buf).Encode(doc); err != nil {
			return fmt.Errorf("fail to marshal doc %s: %w", documentID, err)
		}

		err = indexer.Add(
			ctx,
			opensearchutil.BulkIndexerItem{
				Action:     "index",    // or "create"/"update"/"delete"
				DocumentID: documentID, // "" => auto ID
				Body:       bytes.NewReader(buf.Bytes()),
				OnFailure: func(
					ctx context.Context,
					item opensearchutil.BulkIndexerItem,
					res opensearchapi.BulkRespItem,
					err error,
				) {
					if err != nil {
						e.Logger.Log("msg", fmt.Sprintf("bulk error for %s: %v", item.DocumentID, err))
					}
				},
			},
		)
		if err != nil {
			return fmt.Errorf("add to bulk indexer: %w", err)
		}
	}

	// Wait for all pending operations to flush
	if err := indexer.Close(ctx); err != nil {
		return fmt.Errorf("bulk close: %w", err)
	}

	stats := indexer.Stats()
	e.Logger.Log("msg", fmt.Sprintf("bulk done: indexed=%d failed=%d", stats.NumIndexed, stats.NumFailed))

	return nil
}

func (e *OpenSearchEndpoint) getDocument(indexName string, documentID string) (string, error) {
	ctx := context.Background()
	response, err := e.Client.Document.Get(ctx, opensearchapi.DocumentGetReq{
		Index:      indexName,
		DocumentID: documentID,
	})
	if err != nil {
		return "", fmt.Errorf("error getting document %s from index %s: %v", documentID, indexName, err)
	}

	if response.Inspect().Response.StatusCode != 200 {
		return "", fmt.Errorf("unexpected status code %d when getting document %s from index %s", response.Inspect().Response.StatusCode, documentID, indexName)
	}

	var result map[string]interface{}
	if err := json.NewDecoder(response.Inspect().Response.Body).Decode(&result); err != nil {
		return "", fmt.Errorf("error decoding response body: %v", err)
	}

	source, ok := result["_source"].(map[string]interface{})
	if !ok {
		return "", fmt.Errorf("error retrieving _source from response")
	}

	content, ok := source["content"].(string)
	if !ok {
		return "", fmt.Errorf("error retrieving content from _source")
	}

	return content, nil
}

func (e *OpenSearchEndpoint) getAllIndexDocuments(indexName string) (map[string][]byte, error) {
	ctx := context.Background()
	query := `{"query":{"match_all":{}}}`
	files := make(map[string][]byte)
	maxObjects := 10000

	// Fetch all documents from the index
	resp, err := e.Client.Search(ctx, &opensearchapi.SearchReq{
		Indices: []string{indexName},
		Body:    strings.NewReader(query),
		Params: opensearchapi.SearchParams{
			Size: &maxObjects,
		},
	})
	if err != nil {
		return nil, err
	}

	// Read and parse the response
	type Hit struct {
		ID     string         `json:"_id"`
		Source map[string]any `json:"_source"`
	}
	type Hits struct {
		Hits []Hit `json:"hits"`
	}
	type SearchResp struct {
		Hits Hits `json:"hits"`
	}

	var sr SearchResp
	if err := json.NewDecoder(resp.Inspect().Response.Body).Decode(&sr); err != nil {
		return nil, err
	}

	for _, h := range sr.Hits.Hits {
		files[h.ID] = []byte(h.Source["content"].(string))
	}

	return files, nil
}

func (e *OpenSearchEndpoint) countDocuments(indexName string) (int64, error) {
	ctx := context.Background()
	response, err := e.Client.Indices.Count(ctx, &opensearchapi.IndicesCountReq{
		Indices: []string{indexName},
	})
	if err != nil {
		return 0, fmt.Errorf("error counting documents in index %s: %v", indexName, err)
	}

	if response.Inspect().Response.StatusCode != 200 {
		return 0, fmt.Errorf("unexpected status code %d when counting documents in index %s", response.Inspect().Response.StatusCode, indexName)
	}

	var result map[string]interface{}
	if err := json.NewDecoder(response.Inspect().Response.Body).Decode(&result); err != nil {
		return 0, fmt.Errorf("error decoding response body: %v", err)
	}

	count, ok := result["count"].(float64)
	if !ok {
		return 0, fmt.Errorf("error retrieving count from response")
	}

	return int64(count), nil
}

func (e *OpenSearchEndpoint) deleteDocument(indexName string, documentID string) error {
	ctx := context.Background()
	response, err := e.Client.Document.Delete(ctx, opensearchapi.DocumentDeleteReq{
		Index:      indexName,
		DocumentID: documentID,
	})
	if err != nil {
		return fmt.Errorf("error deleting document %s from index %s: %v", documentID, indexName, err)
	}

	if response.Inspect().Response.StatusCode != 200 {
		return fmt.Errorf("unexpected status code %d when deleting document %s from index %s", response.Inspect().Response.StatusCode, documentID, indexName)
	}

	return nil
}

func (e *OpenSearchEndpoint) catHealth() error {
	ctx := context.Background()
	response, err := e.Client.Cat.Health(ctx, &opensearchapi.CatHealthReq{})
	if err != nil {
		return fmt.Errorf("error getting cat health: %v", err)
	}

	if response.Inspect().Response.StatusCode != 200 {
		return fmt.Errorf("unexpected status code %d when getting cat health", response.Inspect().Response.StatusCode)
	}

	return nil
}

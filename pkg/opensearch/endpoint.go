package opensearch

import (
	"fmt"

	"github.com/go-kit/log"
	"github.com/opensearch-project/opensearch-go/v4/opensearchapi"
)

type OpenSearchEndpoint struct {
	Name         string
	ClusterLevel bool
	ClusterName  string
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

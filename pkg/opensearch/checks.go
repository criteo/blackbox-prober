package opensearch

import (
	"context"
	"fmt"

	"github.com/criteo/blackbox-prober/pkg/topology"
	"github.com/opensearch-project/opensearch-go/v4/opensearchapi"
)

func LatencyPrepare(p topology.ProbeableEndpoint) error {
	return nil
}

func LatencyCheck(p topology.ProbeableEndpoint) error {

	e, ok := p.(*OpenSearchEndpoint)
	if !ok {
		return fmt.Errorf("error: given endpoint is not an opensearch endpoint")
	}
	ctx := context.Background()

	response, err := e.Client.Ping(ctx, &opensearchapi.PingReq{})
	if err != nil {
		return fmt.Errorf("error pinging opensearch endpoint %v: %v", e.ClientConfig.Client.Addresses, err)
	}
	defer response.Body.Close()

	if response.StatusCode != 200 {
		return fmt.Errorf("error: unexpected status code %d when pinging opensearch endpoint %s", response.StatusCode, e.GetHash())
	}

	return nil
}

func DurabilityPrepare(p topology.ProbeableEndpoint) error {
	return nil
}

func DurabilityCheck(p topology.ProbeableEndpoint) error {
	return nil
}

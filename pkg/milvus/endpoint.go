package milvus

import (
	"context"
	"fmt"

	"github.com/go-kit/log"

	mv "github.com/milvus-io/milvus/client/v2/milvusclient"
)

type MilvusEndpoint struct {
	Name         string
	ClusterLevel bool
	ClusterName  string
	Client       *mv.Client
	Config       MilvusClientConfig
	Logger       log.Logger
	Database     string
	// MonitoringDatabase is the database the probe manages (defaults to "monitoring")
	MonitoringDatabase string
}

func (e *MilvusEndpoint) GetHash() string {
	return fmt.Sprintf("%s/%s/db:%s", e.ClusterName, e.Name, e.Database)
}

func (e *MilvusEndpoint) GetName() string {
	return e.Name
}

func (e *MilvusEndpoint) IsCluster() bool {
	return e.ClusterLevel
}

func (e *MilvusEndpoint) Connect() error {
	clientConfig := &mv.ClientConfig{
		Address:        e.Config.Address,
		DBName:         e.Config.DBName,
		EnableTLSAuth:  e.Config.EnableTLSAuth,
		APIKey:         e.Config.APIKey,
		RetryRateLimit: e.Config.RetryRateLimit,
		Username:       e.Config.Username,
		Password:       e.Config.Password,
	}

	client, err := mv.New(context.Background(), clientConfig)
	if err != nil {
		return err
	}
	e.Client = client
	return nil
}

func (e *MilvusEndpoint) Refresh() error {
	return nil
}

func (e *MilvusEndpoint) Close() error {
	if e != nil && e.Client != nil {
		e.Client.Close(context.Background())
	}
	return nil
}

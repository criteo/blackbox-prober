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
	client, err := mv.New(context.Background(), &mv.ClientConfig{
		Address:        e.Config.Address,
		Username:       e.Config.Username,
		Password:       e.Config.Password,
		DBName:         e.Config.DBName,
		EnableTLSAuth:  e.Config.EnableTLSAuth,
		APIKey:         e.Config.APIKey,
		RetryRateLimit: e.Config.RetryRateLimit,
	})
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

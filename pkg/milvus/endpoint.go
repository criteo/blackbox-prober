package milvus

import (
	"context"
	"fmt"
	"time"

	"github.com/go-kit/log"

	mv "github.com/milvus-io/milvus/client/v2/milvusclient"
)

type MilvusEndpoint struct {
	Name         string
	ClusterLevel bool
	ClusterName  string
	Client       *mv.Client
	Config       mv.ClientConfig
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
	// TODO: maybe make timeout configurable? For now hardcoding to 15s should be quite okay
	context, cancel := context.WithTimeout(context.Background(), time.Duration(time.Second*15))
	defer cancel()
	client, err := mv.New(context, &e.Config)
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
		e.Client.Close(context.Background()) // no timeout on close
	}
	return nil
}

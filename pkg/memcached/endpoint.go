package memcached

import (
	"fmt"
	"strings"
	"time"

	"github.com/go-kit/log"

	"github.com/bradfitz/gomemcache/memcache"
)

type MemcachedEndpoint struct {
	Name         string
	ClusterLevel bool
	ClusterName  string
	Config       MemcachedProbeConfig
	Client       *memcache.Client
	Servers      []string
	Logger       log.Logger
}

func (e *MemcachedEndpoint) GetHash() string {
	hash := fmt.Sprintf("%s/%s[%s]", e.ClusterName, e.Name, strings.Join(e.Servers, ":"))
	return hash
}

func (e *MemcachedEndpoint) GetName() string {
	return e.Name
}

func (e *MemcachedEndpoint) IsCluster() bool {
	return e.ClusterLevel
}

func (e *MemcachedEndpoint) Connect() error {
	e.Client = memcache.New(e.Servers...)
	e.Client.Timeout = time.Second * 1
	return nil
}

func (e *MemcachedEndpoint) Refresh() error {
	return nil
}

func (e *MemcachedEndpoint) Close() error {
	return nil
}

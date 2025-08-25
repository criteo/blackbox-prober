package common

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"os"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"

	kingpin "github.com/alecthomas/kingpin/v2"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/prometheus/common/promlog"
	promlogflag "github.com/prometheus/common/promlog/flag"
	"gopkg.in/yaml.v2"
)

type ProbeConfig struct {
	LogConfig      promlog.Config `yaml:"log,omitempty"`
	HttpListenAddr string         `yaml:"http_listen_addr,omitempty"`
	ConfigPath     string         `yaml:"config_path,omitempty"`
}

func AddFlags(a *kingpin.Application, cfg *ProbeConfig) {
	a.HelpFlag.Short('h')
	a.Flag("web.listen-address", "Address to listen on for UI, API, and telemetry.").
		Default("0.0.0.0:8080").StringVar(&cfg.HttpListenAddr)
	a.Flag("config.path", "Path to the probe configuration file").
		Default("conf.yaml").StringVar(&cfg.ConfigPath)
	promlogflag.AddFlags(a, &cfg.LogConfig)
}

func (cfg *ProbeConfig) ParseConfigFile(config interface{}) error {
	logger := cfg.GetLogger()
	level.Info(logger).Log("msg", fmt.Sprintf("Parsing the configuration file (--config.path=%s)", cfg.ConfigPath))
	configData, err := ioutil.ReadFile(cfg.ConfigPath)
	if err != nil {
		level.Error(logger).Log("msg", fmt.Sprintf("Failed to parse the configuration file (--config.path=%s)", cfg.ConfigPath), "err", err)
		os.Exit(2)
	}
	return yaml.Unmarshal(configData, config)
}

func (cfg *ProbeConfig) GetLogger() log.Logger {
	return promlog.New(&cfg.LogConfig)
}

func (cfg *ProbeConfig) StartHttpServer() {
	// Prometheus stuff
	http.HandleFunc("/ready", BasicHealthCheck)
	http.Handle("/metrics", promhttp.Handler())
	go http.ListenAndServe(cfg.HttpListenAddr, nil)
}

func BasicHealthCheck(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(200)
}

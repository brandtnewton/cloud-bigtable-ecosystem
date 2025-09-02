package config

import (
	"cassandra-to-bigtable-proxy/utilities"
	"os"

	"github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/global/types"
)

type yamlProxyConfig struct {
	CassandraToBigtableConfigs yamlCassandraToBigtableConfigs `yaml:"cassandraToBigtableConfigs"`
	Listeners                  []yamlListener                 `yaml:"listeners"`
	Otel                       *yamlOtelConfig                `yaml:"otel"`
	LoggerConfig               *utilities.LoggerConfig        `yaml:"loggerConfig"`
}

// yamlCassandraToBigtableConfigs contains configurations for Cassandra to bigtable proxy
type yamlCassandraToBigtableConfigs struct {
	ProjectID          string `yaml:"projectId"`
	SchemaMappingTable string `yaml:"DefaultSchemaMappingTableName"`
}
type yamlOtelConfig struct {
	Enabled     bool   `yaml:"enabled"`
	ServiceName string `yaml:"serviceName"`
	HealthCheck struct {
		Enabled  bool   `yaml:"enabled"`
		Endpoint string `yaml:"endpoint"`
	} `yaml:"healthcheck"`
	Metrics struct {
		Endpoint string `yaml:"endpoint"`
	} `yaml:"metrics"`
	Traces struct {
		Endpoint      string  `yaml:"endpoint"`
		SamplingRatio float64 `yaml:"samplingRatio"`
	} `yaml:"traces"`
}

// yamlListener represents each listener configuration
type yamlListener struct {
	Name     string       `yaml:"name"`
	Port     int          `yaml:"port"`
	Bigtable yamlBigtable `yaml:"bigtable"`
	Otel     yamlOtel     `yaml:"otel"`
}

// yamlBigtable holds the yamlBigtable database configuration

type yamlInstancesMap struct {
	BigtableInstance string `yaml:"bigtableInstance"`
	Keyspace         string `yaml:"keyspace"`
	AppProfileID     string `yaml:"appProfileID"`
}
type yamlBigtable struct {
	ProjectID                     string             `yaml:"projectId"`
	Instances                     []yamlInstancesMap `yaml:"instances"`
	InstanceIDs                   string             `yaml:"instanceIds"`
	SchemaMappingTable            string             `yaml:"schemaMappingTable"`
	Session                       yamlSession        `yaml:"yamlSession"`
	DefaultColumnFamily           string             `yaml:"defaultColumnFamily"`
	AppProfileID                  string             `yaml:"appProfileID"`
	EncodeIntRowKeysWithBigEndian bool               `yaml:"encodeIntRowKeysWithBigEndian"`
}

// yamlSession describes the settings for yamlBigtable sessions
type yamlSession struct {
	GrpcChannels int `yaml:"grpcChannels"`
}

// yamlOtel configures OpenTelemetry features
type yamlOtel struct {
	Disabled bool `yaml:"disabled"`
}

const defaultConfigFile = "config.yaml"

type rawCliArgs struct {
	Version            bool     `yaml:"version" help:"Show current proxy version" short:"v" default:"false" env:"PROXY_VERSION"`
	RpcAddress         string   `yaml:"rpc-address" help:"Address to advertise in the 'system.local' table for 'rpc_address'. It must be set if configuring peer proxies" env:"RPC_ADDRESS"`
	ProtocolVersion    string   `yaml:"protocol-version" help:"Initial protocol version to use when connecting to the backend cluster (default: v4, options: v3, v4, v5, DSEv1, DSEv2)" default:"v4" short:"n" env:"PROTOCOL_VERSION"`
	MaxProtocolVersion string   `yaml:"max-protocol-version" help:"Max protocol version supported by the backend cluster (default: v4, options: v3, v4, v5, DSEv1, DSEv2)" default:"v4" short:"m" env:"MAX_PROTOCOL_VERSION"`
	DataCenter         string   `yaml:"data-center" help:"Data center to use in system tables" default:"datacenter1"  env:"DATA_CENTER"`
	Bind               string   `yaml:"bind" help:"Address to use to bind server" short:"a" default:":9042" env:"BIND"`
	Config             *os.File `yaml:"-" help:"YAML configuration file" short:"f" env:"CONFIG_FILE"` // Not available in the configuration file
	NumConns           int      `yaml:"num-conns" help:"Number of connection to create to each node of the backend cluster" default:"20" env:"NUM_CONNS"`
	ReleaseVersion     string   `yaml:"release-version" help:"Cluster Release version" default:"4.0.0.6816"  env:"RELEASE_VERSION"`
	Partitioner        string   `yaml:"partitioner" help:"Partitioner partitioner" default:"org.apache.cassandra.dht.Murmur3Partitioner"  env:"PARTITIONER"`
	Tokens             []string `yaml:"tokens" help:"Tokens to use in the system tables. It's not recommended" env:"TOKENS"`
	CQLVersion         string   `yaml:"cql-version" help:"CQL version" default:"3.4.5"  env:"CQLVERSION"`
	LogLevel           string   `yaml:"log-level" help:"Log level configuration." default:"info" env:"LOG_LEVEL"`
	TcpBindPort        string   `yaml:"-" help:"YAML configuration file" short:"t" env:"TCP_BIND_PORT"`
	UseUnixSocket      bool     `help:"Use Unix Domain Socket instead of TCP." default:"false"`
	UnixSocketPath     string   `help:"Path for the Unix Domain Socket file." default:"/tmp/cassandra-proxy.sock"`
	ProxyCertFile      string   `yaml:"proxy-cert-file" help:"Path to a PEM encoded certificate file with its intermediate certificate chain. This is used to encrypt traffic for proxy clients" env:"PROXY_CERT_FILE"`
	ProxyKeyFile       string   `yaml:"proxy-key-file" help:"Path to a PEM encoded private key file. This is used to encrypt traffic for proxy clients" env:"PROXY_KEY_FILE"`
	// hidden because we only intend the java session wrapper to use this flag
	UserAgentOverride string `yaml:"-" help:"" hidden:"" optional:"" default:"" short:"u"`
	ClientPid         int32  `yaml:"client-pid" help:"" hidden:"" optional:"" default:"" short:""`
	ClientUid         uint32 `yaml:"client-uid" help:"" hidden:"" optional:"" default:"" short:""`
	// quick start config
	// todo use
	ProjectId  string `yaml:"project-id" help:"Google Cloud Project Id to use."`
	InstanceId string `yaml:"instance-id" help:"Bigtable Instance Id to use."`
	AppProfile string `yaml:"app-profile" help:"Bigtable App Profile to use."`
}

type CliArgs struct {
	Version            bool
	RpcAddress         string
	ProtocolVersion    string
	MaxProtocolVersion string
	DataCenter         string
	Bind               string
	Config             *os.File
	NumConns           int
	ReleaseVersion     string
	Partitioner        string
	Tokens             []string
	CQLVersion         string
	LogLevel           string
	TcpBindPort        string
	UseUnixSocket      bool
	UnixSocketPath     string
	ProxyCertFile      string
	ProxyKeyFile       string
	// hidden because we only intend the java session wrapper to use this flag
	UserAgentOverride string
	ClientPid         int32
	ClientUid         uint32
	// quick start config
	// todo use
	ProjectId  string
	InstanceId string
	AppProfile string
}

type ProxyConfig struct {
	Listeners    []*Listener
	Otel         *OtelConfig
	LoggerConfig *utilities.LoggerConfig
}

type OtelConfig struct {
	Enabled     bool
	ServiceName string
	HealthCheck struct {
		Enabled  bool
		Endpoint string
	}
	Metrics struct {
		Endpoint string
	}
	Traces struct {
		Endpoint      string
		SamplingRatio float64
	}
}

// Listener represents each listener configuration
type Listener struct {
	Name     string
	Port     int
	Bigtable *Bigtable
	Otel     *Otel
}

type InstancesMapping struct {
	BigtableInstance string
	Keyspace         string
	AppProfileID     string
}

type Bigtable struct {
	ProjectID                string
	Instances                map[string]*InstancesMapping
	SchemaMappingTable       string
	Session                  *Session
	DefaultColumnFamily      string
	DefaultIntRowKeyEncoding types.IntRowKeyEncodingType
	UserAgent                string
}

type Session struct {
	GrpcChannels int
}

type Otel struct {
	Disabled bool
}

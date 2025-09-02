package config

import (
	"os"

	"github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/global/types"
	"github.com/datastax/go-cassandra-native-protocol/primitive"
	"go.uber.org/zap"
)

type yamlProxyConfig struct {
	CassandraToBigtableConfigs yamlCassandraToBigtableConfigs `yaml:"cassandraToBigtableConfigs"`
	Listeners                  []yamlListener                 `yaml:"listeners"`
	Otel                       *yamlOtelConfig                `yaml:"otel"`
	LoggerConfig               *LoggerConfig                  `yaml:"loggerConfig"`
}

type LoggerConfig struct {
	OutputType string `yaml:"outputType"`
	Filename   string `yaml:"fileName"`
	MaxSize    int    `yaml:"maxSize"`    // megabytes
	MaxBackups int    `yaml:"maxBackups"` // The value of MaxBackups determines how many previous log files are kept after a new log file is created due to the MaxSize or MaxAge limits.
	MaxAge     int    `yaml:"maxAge"`     // days
	Compress   bool   `yaml:"compress"`   // the rotated log files to be compressed to save disk space.
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

type CliArgs struct {
	Version            bool
	RpcAddress         string
	ProtocolVersion    primitive.ProtocolVersion
	MaxProtocolVersion primitive.ProtocolVersion
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

type InstancesMapping struct {
	BigtableInstance string
	Keyspace         string
	AppProfileID     string
}

type BigtableConfig struct {
	ProjectID                string
	Instances                map[string]*InstancesMapping
	SchemaMappingTable       string
	Session                  *Session
	DefaultColumnFamily      string
	DefaultIntRowKeyEncoding types.IntRowKeyEncodingType
}

type Session struct {
	GrpcChannels int
}

type ProxyGlobalConfig struct {
	ProtocolVersion primitive.ProtocolVersion
	MaxVersion      primitive.ProtocolVersion
	ReleaseVersion  string
	Partitioner     string
	CQLVersion      string
	UserAgent       string
	CliArgs         *CliArgs
	ClientPid       int32
	ClientUid       uint32
	TcpBindPort     string
	Otel            *OtelConfig
	LoggerConfig    *LoggerConfig
}

type ProxyInstanceConfig struct {
	Port           int
	Bind           string
	GlobalConfig   *ProxyGlobalConfig
	NumConns       int
	Logger         *zap.Logger
	RPCAddr        string
	DC             string
	Tokens         []string
	BigtableConfig *BigtableConfig
	OtelConfig     *OtelConfig
}

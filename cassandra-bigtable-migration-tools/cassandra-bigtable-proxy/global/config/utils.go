package config

import (
	"fmt"
	"os"
	"strings"

	"github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/global/types"
	"github.com/datastax/go-cassandra-native-protocol/primitive"
	"gopkg.in/yaml.v2"
)

var readFile = os.ReadFile

func parseProtocolVersion(s string) (version primitive.ProtocolVersion, ok bool) {
	ok = true
	lowered := strings.ToLower(s)
	if lowered == "3" || lowered == "v3" {
		version = primitive.ProtocolVersion3
	} else if lowered == "4" || lowered == "v4" {
		version = primitive.ProtocolVersion4
	} else if lowered == "5" || lowered == "v5" {
		version = primitive.ProtocolVersion5
	} else if lowered == "65" || lowered == "dsev1" {
		version = primitive.ProtocolVersionDse1
	} else if lowered == "66" || lowered == "dsev2" {
		version = primitive.ProtocolVersionDse1
	} else {
		ok = false
	}
	return version, ok
}

// LoadConfig reads and parses the configuration from a YAML file
func loadProxyConfigFile(filename string, args *CliArgs) (*ProxyGlobalConfig, []*ProxyInstanceConfig, error) {
	data, err := readFile(filename)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to read config file: %w", err)
	}

	var config yamlProxyConfig
	if err = yaml.Unmarshal(data, &config); err != nil {
		return nil, nil, fmt.Errorf("failed to unmarshal config: %w", err)
	}

	// todo unit test with parent configs not defined to ensure no NPE is thrown
	globalConfig := &ProxyGlobalConfig{
		CliArgs: args,
		Otel: &OtelConfig{
			Enabled:     config.Otel.Enabled,
			ServiceName: config.Otel.ServiceName,
			HealthCheck: struct {
				Enabled  bool
				Endpoint string
			}{
				Enabled:  config.Otel.HealthCheck.Enabled,
				Endpoint: config.Otel.HealthCheck.Endpoint,
			},
			Metrics: struct {
				Endpoint string
			}{
				Endpoint: config.Otel.Metrics.Endpoint,
			},
			Traces: struct {
				Endpoint      string
				SamplingRatio float64
			}{
				Endpoint:      config.Otel.Traces.Endpoint,
				SamplingRatio: config.Otel.Traces.SamplingRatio,
			},
		},
		LoggerConfig: &LoggerConfig{
			OutputType: config.LoggerConfig.OutputType,
			Filename:   config.LoggerConfig.Filename,
			MaxSize:    config.LoggerConfig.MaxSize,
			MaxBackups: config.LoggerConfig.MaxBackups,
			MaxAge:     config.LoggerConfig.MaxAge,
			Compress:   config.LoggerConfig.Compress,
		},
	}

	var instanceConfigs []*ProxyInstanceConfig = nil
	for _, l := range config.Listeners {
		listener, err := loadListenerConfig(&l, &config, globalConfig)
		if err != nil {
			return nil, nil, err
		}
		instanceConfigs = append(instanceConfigs, listener)
	}

	return globalConfig, instanceConfigs, nil
}

func loadListenerConfig(l *yamlListener, config *yamlProxyConfig, globalConfig *ProxyGlobalConfig) (*ProxyInstanceConfig, error) {
	projectId := l.Bigtable.ProjectID
	if projectId == "" {
		projectId = config.CassandraToBigtableConfigs.ProjectID
	}
	schemaMappingTable := l.Bigtable.SchemaMappingTable
	if schemaMappingTable == "" {
		schemaMappingTable = config.CassandraToBigtableConfigs.SchemaMappingTable
	}
	var defaultAppProfileId = l.Bigtable.AppProfileID
	if defaultAppProfileId == "" {
		defaultAppProfileId = "default"
	}

	var instancesDefined = len(l.Bigtable.Instances) > 0
	var instanceIdsDefined = l.Bigtable.InstanceIDs != ""
	if !instanceIdsDefined && !instancesDefined {
		return nil, fmt.Errorf("either 'instances' or 'instance_ids' must be defined for listener %s on port %d", l.Name, l.Port)
	}
	if instanceIdsDefined && instancesDefined {
		return nil, fmt.Errorf("only one of 'instances' or 'instance_ids' should be set for listener %s on port %d", l.Name, l.Port)
	}
	var instances = make(map[string]*InstancesMapping)
	if len(l.Bigtable.Instances) != 0 {
		for _, i := range l.Bigtable.Instances {
			appProfileId := i.AppProfileID
			if appProfileId == "" {
				appProfileId = defaultAppProfileId
			}
			instances[i.Keyspace] = &InstancesMapping{
				BigtableInstance: i.BigtableInstance,
				Keyspace:         i.Keyspace,
				AppProfileID:     appProfileId,
			}
		}
	} else {
		instanceIds := strings.Split(l.Bigtable.InstanceIDs, ",")
		for _, id := range instanceIds {
			id := strings.TrimSpace(id)
			instances[id] = &InstancesMapping{
				BigtableInstance: id,
				Keyspace:         id,
				AppProfileID:     defaultAppProfileId,
			}
		}
	}

	intRowKeyEncoding := types.OrderedCodeEncoding
	if l.Bigtable.EncodeIntRowKeysWithBigEndian {
		intRowKeyEncoding = types.BigEndianEncoding
	}

	bigtableConfig := &BigtableConfig{
		ProjectID:          projectId,
		Instances:          instances,
		SchemaMappingTable: schemaMappingTable,
		Session: &Session{
			GrpcChannels: l.Bigtable.Session.GrpcChannels,
		},
		DefaultColumnFamily:      l.Bigtable.DefaultColumnFamily,
		DefaultIntRowKeyEncoding: intRowKeyEncoding,
	}

	return NewProxyInstanceConfig(globalConfig, l.Port, bigtableConfig), nil
}

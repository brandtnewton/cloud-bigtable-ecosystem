package config

import (
	"fmt"
	"os"
	"strings"

	"github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/global/types"
	"github.com/datastax/go-cassandra-native-protocol/primitive"
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
func loadProxyConfigFile(config *yamlProxyConfig, args *types.CliArgs) ([]*types.ProxyInstanceConfig, error) {

	if config.Otel == nil {
		config.Otel = &yamlOtelConfig{
			Enabled: false,
		}
	}

	if config.LoggerConfig == nil {
		config.LoggerConfig = &yamlLoggerConfig{
			OutputType: "",
			Filename:   "",
			MaxSize:    0,
			MaxBackups: 0,
			MaxAge:     0,
			Compress:   false,
		}
	}

	// todo unit test with parent configs not defined to ensure no NPE is thrown
	otel := &types.OtelConfig{
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
	}

	if otel.Enabled {
		if otel.Traces.SamplingRatio < 0 || otel.Traces.SamplingRatio > 1 {
			return nil, fmt.Errorf("sampling ratio for otel traces should be between 0 and 1]")
		}
	}

	var instanceConfigs []*types.ProxyInstanceConfig = nil
	for _, l := range config.Listeners {
		listener, err := loadListenerConfig(args, &l, config, otel)
		if err != nil {
			return nil, err
		}
		instanceConfigs = append(instanceConfigs, listener)
	}

	return instanceConfigs, nil
}

func loadListenerConfig(args *types.CliArgs, l *yamlListener, config *yamlProxyConfig, otel *types.OtelConfig) (*types.ProxyInstanceConfig, error) {
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
	var instances = make(map[string]*types.InstancesMapping)
	if len(l.Bigtable.Instances) != 0 {
		for _, i := range l.Bigtable.Instances {
			appProfileId := i.AppProfileID
			if appProfileId == "" {
				appProfileId = defaultAppProfileId
			}
			instances[i.Keyspace] = &types.InstancesMapping{
				BigtableInstance: i.BigtableInstance,
				Keyspace:         i.Keyspace,
				AppProfileID:     appProfileId,
			}
		}
	} else {
		instanceIds := strings.Split(l.Bigtable.InstanceIDs, ",")
		for _, id := range instanceIds {
			id := strings.TrimSpace(id)
			instances[id] = &types.InstancesMapping{
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

	bigtableConfig := &types.BigtableConfig{
		ProjectID:          projectId,
		Instances:          instances,
		SchemaMappingTable: schemaMappingTable,
		Session: &types.Session{
			GrpcChannels: l.Bigtable.Session.GrpcChannels,
		},
		DefaultColumnFamily:      l.Bigtable.DefaultColumnFamily,
		DefaultIntRowKeyEncoding: intRowKeyEncoding,
	}

	return NewProxyInstanceConfig(args, l.Port, otel, bigtableConfig), nil
}

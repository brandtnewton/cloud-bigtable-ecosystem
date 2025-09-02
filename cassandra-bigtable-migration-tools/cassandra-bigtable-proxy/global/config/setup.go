package config

import (
	"errors"
	"fmt"

	"github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/global/constants"
	"github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/global/types"
	"github.com/alecthomas/kong"
)

const (
	defaultClusterPartitioner = "org.apache.cassandra.dht.Murmur3Partitioner"
	clusterReleaseVersion     = "4.0.0.6816"
	defaultCqlVersion         = "3.4.5"
)

func ParseCliArgs(args []string) (*CliArgs, error) {
	var parsed rawCliArgs

	parser, err := kong.New(&parsed)
	if err != nil {
		return nil, err
	}

	if _, err = parser.Parse(args); err != nil {
		return nil, fmt.Errorf("error parsing flags: %v", err)
	}

	if parsed.Partitioner == "" {
		parsed.Partitioner = defaultClusterPartitioner
	}

	if parsed.ReleaseVersion == "" {
		parsed.ReleaseVersion = clusterReleaseVersion
	}

	if parsed.CQLVersion == "" {
		parsed.CQLVersion = defaultCqlVersion
	}

	err = validateCliArgs(&parsed)
	if err != nil {
		return nil, err
	}

	result := CliArgs{
		Version:            parsed.Version,
		RpcAddress:         parsed.RpcAddress,
		ProtocolVersion:    parsed.ProtocolVersion,
		MaxProtocolVersion: parsed.MaxProtocolVersion,
		DataCenter:         parsed.DataCenter,
		Bind:               parsed.Bind,
		Config:             parsed.Config,
		NumConns:           parsed.NumConns,
		ReleaseVersion:     parsed.ReleaseVersion,
		Partitioner:        parsed.Partitioner,
		Tokens:             parsed.Tokens,
		CQLVersion:         parsed.CQLVersion,
		LogLevel:           parsed.LogLevel,
		TcpBindPort:        parsed.TcpBindPort,
		UseUnixSocket:      parsed.UseUnixSocket,
		UnixSocketPath:     parsed.UnixSocketPath,
		ProxyCertFile:      parsed.ProxyCertFile,
		ProxyKeyFile:       parsed.ProxyKeyFile,
		UserAgentOverride:  parsed.UserAgentOverride,
		ClientPid:          parsed.ClientPid,
		ClientUid:          parsed.ClientUid,
		ProjectId:          parsed.ProjectId,
		InstanceId:         parsed.InstanceId,
		AppProfile:         parsed.AppProfile,
	}

	return &result, err
}

func isQuickStartArgsDefined(args *CliArgs) bool {
	return args.ProjectId != "" || args.AppProfile != "" || args.InstanceId != ""
}

func parseQuickStartArgs(args *CliArgs) (*ProxyConfig, error) {
	if !isQuickStartArgsDefined(args) {
		return nil, errors.New("no quickstart args found")
	}

	return &ProxyConfig{
		Listeners: []*Listener{
			{
				Name: "default-listener",
				Port: 9042,
				Bigtable: &Bigtable{
					ProjectID: args.ProjectId,
					Instances: map[string]*InstancesMapping{
						args.InstanceId: {
							BigtableInstance: args.InstanceId,
							Keyspace:         args.InstanceId,
							AppProfileID:     args.AppProfile,
						},
					},
					SchemaMappingTable: DefaultSchemaMappingTableName,
					Session: &Session{
						GrpcChannels: DefaultBigtableGrpcChannels,
					},
					DefaultColumnFamily:      DefaultColumnFamily,
					DefaultIntRowKeyEncoding: types.OrderedCodeEncoding,
					UserAgent:                getUserAgent(args),
				},
			},
		},
		Otel: &OtelConfig{
			Enabled: false,
		},
		LoggerConfig: nil,
	}, nil
}

func getUserAgent(args *CliArgs) string {
	userAgent := "cassandra-adapter/" + constants.ProxyReleaseVersion
	if args.UserAgentOverride != "" {
		userAgent = args.UserAgentOverride
	}
	return userAgent
}

func ParseProxyConfig(args *CliArgs) (*ProxyConfig, error) {
	if isQuickStartArgsDefined(args) {
		// todo polish UX - is this good?
		fmt.Printf("quick start cli args found. continuing with quickstart config instead of reading config file.\n")
		return parseQuickStartArgs(args)
	}

	if args.Config == nil {
		return nil, errors.New("no config file provided")
	}

	proxyConfig, err := loadProxyConfig(args.Config.Name(), args)
	if err != nil {
		return nil, fmt.Errorf("error while loading config.yaml: %w", err)
	}

	return proxyConfig, nil
}

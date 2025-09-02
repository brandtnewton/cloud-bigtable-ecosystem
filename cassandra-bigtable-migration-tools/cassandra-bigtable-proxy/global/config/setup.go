package config

import (
	"errors"
	"fmt"
	"net"
	"os"

	"github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/global/constants"
	"github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/global/types"
	"github.com/alecthomas/kong"
	"github.com/datastax/go-cassandra-native-protocol/primitive"
)

const (
	defaultClusterPartitioner = "org.apache.cassandra.dht.Murmur3Partitioner"
	clusterReleaseVersion     = "4.0.0.6816"
	defaultCqlVersion         = "3.4.5"
	defaultTcpBindPort        = "0.0.0.0:%s"
)

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
	ProjectId  string `yaml:"project-id" help:"Google Cloud Project Id to use."`
	InstanceId string `yaml:"instance-id" help:"BigtableConfig Instance Id to use."`
	AppProfile string `yaml:"app-profile" help:"BigtableConfig App Profile to use."`
}

func parseCliArgs(args []string) (*CliArgs, error) {
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

	var ok bool
	var version primitive.ProtocolVersion
	if version, ok = parseProtocolVersion(parsed.ProtocolVersion); !ok {
		return nil, fmt.Errorf("unsupported protocol version: %s", parsed.ProtocolVersion)
	}

	var maxVersion primitive.ProtocolVersion
	if maxVersion, ok = parseProtocolVersion(parsed.MaxProtocolVersion); !ok {
		return nil, fmt.Errorf("unsupported max protocol version: %s", parsed.ProtocolVersion)
	}

	if version > maxVersion {
		return nil, fmt.Errorf("default protocol version is greater than max protocol version")
	}

	result := CliArgs{
		Version:            parsed.Version,
		RpcAddress:         parsed.RpcAddress,
		ProtocolVersion:    version,
		MaxProtocolVersion: maxVersion,
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

func parseQuickStartArgs(args *CliArgs) (*ProxyGlobalConfig, []*ProxyInstanceConfig, error) {
	if !isQuickStartArgsDefined(args) {
		return nil, nil, errors.New("no quickstart args found")
	}

	globalConfig, err := parseGlobalConfig(args)
	if err != nil {
		return nil, nil, err
	}

	bigtableConfig := &BigtableConfig{
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
	}

	instanceConfigs := []*ProxyInstanceConfig{
		NewProxyInstanceConfig(globalConfig, 9042, bigtableConfig),
	}

	return globalConfig, instanceConfigs, nil
}

func parseGlobalConfig(args *CliArgs) (*ProxyGlobalConfig, error) {
	tcpPort := args.TcpBindPort
	if tcpPort == "" {
		tcpPort = defaultTcpBindPort
	}

	globalConfig := &ProxyGlobalConfig{
		ProtocolVersion: args.ProtocolVersion,
		MaxVersion:      args.MaxProtocolVersion,
		UserAgent:       getUserAgent(args),
		TcpBindPort:     tcpPort,
		CliArgs:         args,
		ClientPid:       args.ClientPid,
		ClientUid:       args.ClientUid,
		Partitioner:     args.Partitioner,
		ReleaseVersion:  args.ReleaseVersion,
		CQLVersion:      args.CQLVersion,
		Otel: &OtelConfig{
			Enabled: false,
		},
		LoggerConfig: nil,
	}

	if globalConfig.Otel == nil {
		globalConfig.Otel = &OtelConfig{
			Enabled: false,
		}
	} else {
		if globalConfig.Otel.Enabled {
			if globalConfig.Otel.Traces.SamplingRatio < 0 || globalConfig.Otel.Traces.SamplingRatio > 1 {
				return nil, fmt.Errorf("Sampling Ratio for Otel Traces should be between 0 and 1]")
			}
		}
	}

	return globalConfig, nil
}

// maybeAddPort adds the default port to an IP; otherwise, it returns the original address.
func maybeAddPort(addr string, defaultPort string) string {
	if net.ParseIP(addr) != nil {
		return net.JoinHostPort(addr, defaultPort)
	}
	return addr
}

func getUserAgent(args *CliArgs) string {
	userAgent := "cassandra-adapter/" + constants.ProxyReleaseVersion
	if args.UserAgentOverride != "" {
		userAgent = args.UserAgentOverride
	}
	return userAgent
}

func ParseProxyConfigFromArgs(args []string) (*ProxyGlobalConfig, []*ProxyInstanceConfig, error) {
	cliArgs, err := parseCliArgs(args)
	if err != nil {
		return nil, nil, err
	}
	return ParseProxyConfig(cliArgs)
}

func ParseProxyConfig(args *CliArgs) (*ProxyGlobalConfig, []*ProxyInstanceConfig, error) {
	if isQuickStartArgsDefined(args) {
		// todo polish UX - is this good?
		fmt.Printf("quick start cli args found. continuing with quickstart config instead of reading config file.\n")
		return parseQuickStartArgs(args)
	}

	if args.Config == nil {
		return nil, nil, errors.New("no config file provided")
	}

	proxyConfig, instanceConfigs, err := loadProxyConfigFile(args.Config.Name(), args)
	if err != nil {
		return nil, nil, fmt.Errorf("error while loading config.yaml: %w", err)
	}

	return proxyConfig, instanceConfigs, nil
}

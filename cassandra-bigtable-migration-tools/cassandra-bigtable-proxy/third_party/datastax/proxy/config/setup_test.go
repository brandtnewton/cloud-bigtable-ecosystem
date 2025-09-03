package config

import (
	"log"
	"os"
	"testing"

	"github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/global/types"
	"github.com/datastax/go-cassandra-native-protocol/primitive"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseCliArgs(t *testing.T) {

	wd, err := os.Getwd()
	if err != nil {
		log.Fatalf("could not get current working directory: %v", err)
	}

	tests := []struct {
		name    string
		args    []string
		want    *types.CliArgs
		wantErr string
	}{
		{
			name: "Valid config file",
			args: []string{"-f", "testdata/valid_config.yaml", "--num-conns", "19", "--protocol-version", "3"},
			want: &types.CliArgs{
				Version:                       false,
				RpcAddress:                    "",
				ProtocolVersion:               primitive.ProtocolVersion3,
				MaxProtocolVersion:            primitive.ProtocolVersion4,
				DataCenter:                    "datacenter1",
				Bind:                          ":9042",
				ConfigFilePath:                wd + "/testdata/valid_config.yaml",
				NumConns:                      19,
				ReleaseVersion:                "4.0.0.6816",
				Partitioner:                   "org.apache.cassandra.dht.Murmur3Partitioner",
				Tokens:                        nil,
				CQLVersion:                    "3.4.5",
				LogLevel:                      "info",
				TcpBindPort:                   "",
				UseUnixSocket:                 false,
				UnixSocketPath:                "/tmp/cassandra-proxy.sock",
				ProxyCertFile:                 "",
				ProxyKeyFile:                  "",
				UserAgent:                     "",
				ClientPid:                     0,
				ClientUid:                     0,
				QuickStartProjectId:           "",
				QuickStartInstanceId:          "",
				QuickStartAppProfile:          "",
				QuickStartPort:                0,
				QuickStartDefaultColumnFamily: "",
				QuickStartSchemaMappingTable:  "",
			},
			wantErr: "",
		},
		{
			name: "Version flag",
			args: []string{"-v"},
			want: &types.CliArgs{
				Version:                       true,
				RpcAddress:                    "",
				ProtocolVersion:               primitive.ProtocolVersion4,
				MaxProtocolVersion:            primitive.ProtocolVersion4,
				DataCenter:                    "datacenter1",
				Bind:                          ":9042",
				ConfigFilePath:                "",
				NumConns:                      20,
				ReleaseVersion:                "4.0.0.6816",
				Partitioner:                   "org.apache.cassandra.dht.Murmur3Partitioner",
				Tokens:                        nil,
				CQLVersion:                    "3.4.5",
				LogLevel:                      "info",
				TcpBindPort:                   "",
				UseUnixSocket:                 false,
				UnixSocketPath:                "/tmp/cassandra-proxy.sock",
				ProxyCertFile:                 "",
				ProxyKeyFile:                  "",
				UserAgent:                     "",
				ClientPid:                     0,
				ClientUid:                     0,
				QuickStartProjectId:           "",
				QuickStartInstanceId:          "",
				QuickStartAppProfile:          "",
				QuickStartPort:                0,
				QuickStartDefaultColumnFamily: "",
				QuickStartSchemaMappingTable:  "",
			},
			wantErr: "",
		},
		{
			name: "Quick start args",
			args: []string{
				"-f", "testdata/valid_config.yaml",
				"--project-id", "my-project-id",
				"--instance-id", "my-instance-id",
				"--app-profile", "my-app-profile",
				"--port", "9041",
				"--default-column-family", "my-default-column-family",
				"--schema-mapping-table", "my-schema-mapping-table",
			},
			want: &types.CliArgs{
				Version:                       false,
				RpcAddress:                    "",
				ProtocolVersion:               primitive.ProtocolVersion4,
				MaxProtocolVersion:            primitive.ProtocolVersion4,
				DataCenter:                    "datacenter1",
				Bind:                          ":9042",
				ConfigFilePath:                wd + "/testdata/valid_config.yaml",
				NumConns:                      20,
				ReleaseVersion:                "4.0.0.6816",
				Partitioner:                   "org.apache.cassandra.dht.Murmur3Partitioner",
				Tokens:                        nil,
				CQLVersion:                    "3.4.5",
				LogLevel:                      "info",
				TcpBindPort:                   "",
				UseUnixSocket:                 false,
				UnixSocketPath:                "/tmp/cassandra-proxy.sock",
				ProxyCertFile:                 "",
				ProxyKeyFile:                  "",
				UserAgent:                     "",
				ClientPid:                     0,
				ClientUid:                     0,
				QuickStartProjectId:           "my-project-id",
				QuickStartInstanceId:          "my-instance-id",
				QuickStartAppProfile:          "my-app-profile",
				QuickStartPort:                9041,
				QuickStartDefaultColumnFamily: "my-default-column-family",
				QuickStartSchemaMappingTable:  "my-schema-mapping-table",
			},
			wantErr: "",
		},
		{
			name:    "unrecognized flag",
			args:    []string{"--blah"},
			want:    nil,
			wantErr: "unknown flag --blah",
		},
		{
			name:    "invalid num connections",
			args:    []string{"--num-conns", "0"},
			want:    nil,
			wantErr: "invalid number of connections",
		},
		{
			name:    "invalid protocol",
			args:    []string{"--protocol-version", "foo"},
			want:    nil,
			wantErr: "unsupported protocol version: foo",
		},
		{
			name:    "invalid max protocol",
			args:    []string{"--max-protocol-version", "foo"},
			want:    nil,
			wantErr: "unsupported max protocol version: foo",
		},
		{
			name:    "invalid protocol config",
			args:    []string{"--max-protocol-version", "3", "--protocol-version", "4"},
			want:    nil,
			wantErr: "default protocol version is greater than max protocol version",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseCliArgs(tt.args)
			if tt.wantErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErr)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestParseProxyConfig(t *testing.T) {

	wd, err := os.Getwd()
	if err != nil {
		log.Fatalf("could not get current working directory: %v", err)
	}

	tests := []struct {
		name    string
		args    *types.CliArgs
		want    []*types.ProxyInstanceConfig
		wantErr string
	}{
		{
			name: "Valid config file",
			args: &types.CliArgs{
				Version:                       false,
				RpcAddress:                    "",
				ProtocolVersion:               primitive.ProtocolVersion3,
				MaxProtocolVersion:            primitive.ProtocolVersion4,
				DataCenter:                    "datacenter1",
				Bind:                          ":9042",
				ConfigFilePath:                wd + "/testdata/valid_config.yaml",
				NumConns:                      19,
				ReleaseVersion:                "4.0.0.6816",
				Partitioner:                   "org.apache.cassandra.dht.Murmur3Partitioner",
				Tokens:                        nil,
				CQLVersion:                    "3.4.5",
				LogLevel:                      "info",
				TcpBindPort:                   defaultTcpBindPort,
				UseUnixSocket:                 false,
				UnixSocketPath:                "/tmp/cassandra-proxy.sock",
				ProxyCertFile:                 "",
				ProxyKeyFile:                  "",
				UserAgent:                     "proxy",
				ClientPid:                     0,
				ClientUid:                     0,
				QuickStartProjectId:           "",
				QuickStartInstanceId:          "",
				QuickStartAppProfile:          "",
				QuickStartPort:                0,
				QuickStartDefaultColumnFamily: "",
				QuickStartSchemaMappingTable:  "",
			},
			want: []*types.ProxyInstanceConfig{
				{
					Port:     9092,
					Bind:     "0.0.0.0:9092",
					CliArgs:  nil, // reference fixed by test fixture
					NumConns: 19,
					Logger:   nil,
					RPCAddr:  "",
					DC:       "datacenter1",
					Tokens:   nil,
					BigtableConfig: &types.BigtableConfig{
						ProjectID: "cassandra-prod-789",
						Instances: map[string]*types.InstancesMapping{
							"prodinstance001": {
								BigtableInstance: "prod-instance-001",
								Keyspace:         "prodinstance001",
								AppProfileID:     "prod-profile-123",
							},
						},
						SchemaMappingTable: "prod_table_config",
						Session: &types.Session{
							GrpcChannels: 3,
						},
						DefaultColumnFamily:      "cf_default",
						DefaultIntRowKeyEncoding: types.OrderedCodeEncoding,
					},
					OtelConfig: &types.OtelConfig{
						Enabled: false,
					},
				},
			},
			wantErr: "",
		},
		{
			name: "invalid protocol config",
			args: &types.CliArgs{
				ConfigFilePath: wd + "/testdata/invalid_config.yaml",
			},
			want:    nil,
			wantErr: "failed to unmarshal config",
		},
		{
			name: "no listeners provided",
			args: &types.CliArgs{
				ConfigFilePath: wd + "/testdata/no_listeners_config.yaml",
			},
			want:    nil,
			wantErr: "no listeners provided",
		},
		{
			name: "no listeners in yaml but quick start args given",
			args: &types.CliArgs{
				ConfigFilePath:       wd + "/testdata/no_listeners_config.yaml",
				QuickStartPort:       9042,
				QuickStartProjectId:  "my-project",
				QuickStartInstanceId: "my-instance",
			},
			want:    nil,
			wantErr: "",
		},
		{
			name: "quick start",
			args: &types.CliArgs{
				ConfigFilePath:                wd + "/testdata/no_listeners_config.yaml",
				QuickStartPort:                1234,
				QuickStartProjectId:           "my-project",
				QuickStartInstanceId:          "my-instance",
				QuickStartSchemaMappingTable:  "sm",
				QuickStartDefaultColumnFamily: "df",
				QuickStartAppProfile:          "cql-proxy",
			},
			want:    nil,
			wantErr: "",
		},
		{
			name: "missing quickstart arg",
			args: &types.CliArgs{
				QuickStartInstanceId: "my-instance",
			},
			want:    nil,
			wantErr: "partial quickstart config provided",
		},
		{
			name: "missing quickstart arg 2",
			args: &types.CliArgs{
				QuickStartProjectId: "my-instance",
			},
			want:    nil,
			wantErr: "partial quickstart config provided",
		},
		{
			name: "missing quickstart arg 3",
			args: &types.CliArgs{
				QuickStartPort: 0,
			},
			want:    nil,
			wantErr: "partial quickstart config provided",
		},
		{
			name: "quick start and yaml have same port",
			args: &types.CliArgs{
				ConfigFilePath:                wd + "/testdata/valid_config.yaml",
				QuickStartPort:                9092,
				QuickStartProjectId:           "my-project",
				QuickStartInstanceId:          "my-instance",
				QuickStartSchemaMappingTable:  "sm",
				QuickStartDefaultColumnFamily: "df",
				QuickStartAppProfile:          "cql-proxy",
			},
			want:    nil,
			wantErr: "multiple listeners configured to use same port: 9092",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// fix references
			for _, c := range tt.want {
				c.CliArgs = tt.args
			}

			got, err := ParseProxyConfig(tt.args)

			if tt.wantErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErr)
				return
			}

			require.NoError(t, err)
			require.Equal(t, len(tt.want), len(got))
			// loop over each element because this is a clearer diff than assert.ElementsMatch
			for i := range tt.want {
				assert.Equal(t, tt.want[i], got[i])
			}
		})
	}
}

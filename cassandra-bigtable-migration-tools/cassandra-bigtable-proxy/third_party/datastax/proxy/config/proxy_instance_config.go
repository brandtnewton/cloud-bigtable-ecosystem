package config

import (
	"fmt"
	"strconv"

	"github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/global/types"
)

func NewProxyInstanceConfig(cliArgs *types.CliArgs, port int, otel *types.OtelConfig, bigtableConfig *types.BigtableConfig) *types.ProxyInstanceConfig {
	portStr := strconv.Itoa(port)
	bind := fmt.Sprintf(cliArgs.TcpBindPort, portStr)
	bind = maybeAddPort(bind, portStr)

	config := &types.ProxyInstanceConfig{
		Port:     port,
		Bind:     bind,
		CliArgs:  cliArgs,
		NumConns: cliArgs.NumConns,
		// todo init logger
		Logger:         nil,
		RPCAddr:        cliArgs.RpcAddress,
		DC:             cliArgs.DataCenter,
		Tokens:         cliArgs.Tokens,
		OtelConfig:     otel,
		BigtableConfig: bigtableConfig,
	}
	return config
}

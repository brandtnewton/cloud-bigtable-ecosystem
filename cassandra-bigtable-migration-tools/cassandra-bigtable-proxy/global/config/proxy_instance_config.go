package config

import (
	"fmt"
	"strconv"
)

func NewProxyInstanceConfig(globalConfig *ProxyGlobalConfig, port int, bigtableConfig *BigtableConfig) *ProxyInstanceConfig {
	bind := fmt.Sprintf(globalConfig.TcpBindPort, strconv.Itoa(port))
	bind = maybeAddPort(bind, "9042")

	config := &ProxyInstanceConfig{
		Port:         port,
		Bind:         bind,
		GlobalConfig: globalConfig,
		NumConns:     globalConfig.CliArgs.NumConns,
		// todo init logger
		Logger:  nil,
		RPCAddr: globalConfig.CliArgs.RpcAddress,
		DC:      globalConfig.CliArgs.DataCenter,
		Tokens:  globalConfig.CliArgs.Tokens,
		// todo init otel
		OtelConfig:     nil,
		BigtableConfig: bigtableConfig,
	}
	return config
}

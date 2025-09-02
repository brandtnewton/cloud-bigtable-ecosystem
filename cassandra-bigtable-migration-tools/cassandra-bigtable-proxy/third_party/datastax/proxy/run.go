// Copyright (c) DataStax, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package proxy

import (
	"cassandra-to-bigtable-proxy/utilities"
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"net/http"
	"os"
	"strconv"
	"sync"

	"github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/global/config"
	"github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/global/constants"
	"go.uber.org/zap"
)

var (
	defaultTcpBindPort = "0.0.0.0:%s"
)

// Run starts the proxy command. 'args' shouldn't include the executable (i.e. os.Args[1:]). It returns the exit code
// for the proxy.
func Run(ctx context.Context, args []string) error {
	cliArgs, err := config.ParseCliArgs(args)

	if err != nil {
		return err
	}

	proxyConfig, err := config.ParseProxyConfig(cliArgs)
	if err != nil {
		return err
	}

	flag := false
	supportedLogLevels := []string{"info", "debug", "error", "warn"}
	for _, level := range supportedLogLevels {
		if cliArgs.LogLevel == level {
			flag = true
		}
	}
	if !flag {
		return fmt.Errorf("Invalid log-level should be [info/debug/error/warn]")
	}

	logger, err := utilities.SetupLogger(cliArgs.LogLevel, proxyConfig.LoggerConfig)
	if err != nil {
		return fmt.Errorf("unable to create logger")
	}
	defer logger.Sync()
	if cliArgs.Version {
		fmt.Printf("Version - %s\n", constants.ProxyReleaseVersion)
		return nil
	}

	if proxyConfig.Otel == nil {
		proxyConfig.Otel = &config.OtelConfig{
			Enabled: false,
		}
	} else {
		if proxyConfig.Otel.Enabled {
			if proxyConfig.Otel.Traces.SamplingRatio < 0 || proxyConfig.Otel.Traces.SamplingRatio > 1 {
				return fmt.Errorf("Sampling Ratio for Otel Traces should be between 0 and 1]")
			}
		}
	}

	// config logs.
	logger.Info("Protocol Version:" + cliArgs.Version)
	logger.Info("CQL Version:" + cliArgs.CQLVersion)
	logger.Info("Release Version:" + cliArgs.ReleaseVersion)
	logger.Info("Partitioner:" + cliArgs.Partitioner)
	logger.Info("Data Center:" + cliArgs.DataCenter)
	logger.Debug("Configuration - ", zap.Any("ProxyConfig", proxyConfig))
	var wg sync.WaitGroup

	for _, listener := range proxyConfig.Listeners {

		p, err1 := NewProxy(ctx, Config{
			Version:        version,
			MaxVersion:     maxVersion,
			NumConns:       cliArgs.NumConns,
			Logger:         logger,
			RPCAddr:        cliArgs.RpcAddress,
			DC:             cliArgs.DataCenter,
			Tokens:         cliArgs.Tokens,
			BigtableConfig: listener.Bigtable,
			Partitioner:    partitioner,
			ReleaseVersion: releaseVersion,
			CQLVersion:     cqlVersion,
			OtelConfig:     proxyConfig.Otel,
			UserAgent:      listener.Bigtable.UserAgent,
			ClientPid:      cliArgs.ClientPid,
			ClientUid:      cliArgs.ClientUid,
		})

		if err1 != nil {
			logger.Error(err1.Error())
			return err1
		}
		tcpPort := defaultTcpBindPort
		if cliArgs.TcpBindPort != "" {
			tcpPort = cliArgs.TcpBindPort
		}
		cfgloop := cliArgs
		cfgloop.Bind = fmt.Sprintf(tcpPort, strconv.Itoa(listener.Port))
		cfgloop.Bind = maybeAddPort(cfgloop.Bind, "9042")

		var mux http.ServeMux
		wg.Add(1)
		go func(cfg *config.rawCliArgs, p *Proxy, mux *http.ServeMux) {
			defer wg.Done()
			err := listenAndServe(cfg, p, mux, ctx, logger) // Use cfg2 or other instances as needed
			if err != nil {
				logger.Fatal("Error while serving - ", zap.Error(err))
			}
		}(cfgloop, p, &mux)

	}
	wg.Wait() // Wait for all servers to finish
	logger.Debug("\n>>>>>>>>>>>>> Closed All listeners <<<<<<<<<\n")

	return nil
}

// maybeAddPort adds the default port to an IP; otherwise, it returns the original address.
func maybeAddPort(addr string, defaultPort string) string {
	if net.ParseIP(addr) != nil {
		return net.JoinHostPort(addr, defaultPort)
	}
	return addr
}

// listenAndServe correctly handles serving both the proxy and an HTTP server simultaneously.
func listenAndServe(c *config.rawCliArgs, p *Proxy, mux *http.ServeMux, ctx context.Context, logger *zap.Logger) (err error) {
	logger.Info("Starting proxy with configuration:\n")
	logger.Info(fmt.Sprintf("  Bind: %s\n", c.Bind))
	logger.Info(fmt.Sprintf("  Use Unix Socket: %v\n", c.UseUnixSocket))
	logger.Info(fmt.Sprintf("  Unix Socket Path: %s\n", c.UnixSocketPath))
	logger.Info(fmt.Sprintf("  Use TLS: %v\n", c.ProxyCertFile != "" && c.ProxyKeyFile != ""))

	var listeners []net.Listener

	// Set up listener based on configuration
	if c.UseUnixSocket {
		// Use Unix Domain Socket
		unixListener, err := resolveAndListen("", true, c.UnixSocketPath, "", "", logger)
		if err != nil {
			return fmt.Errorf("failed to create Unix socket listener: %v", err)
		}
		listeners = append(listeners, unixListener)
		logger.Info(fmt.Sprintf("Unix socket listener created successfully at %s\n", c.UnixSocketPath))
	} else {
		// Use TCP
		tcpListener, err := resolveAndListen(c.Bind, false, "", c.ProxyCertFile, c.ProxyKeyFile, logger)
		if err != nil {
			return fmt.Errorf("failed to create TCP listener: %v", err)
		}
		listeners = append(listeners, tcpListener)
		logger.Info(fmt.Sprintf("TCP listener created successfully on %s\n", c.Bind))
	}

	// Set up Bigtable client
	logger.Info("Initializing Bigtable client...\n")
	err = p.Connect()
	if err != nil {
		for _, l := range listeners {
			l.Close()
		}
		return err
	}
	logger.Info("Bigtable client initialized successfully\n")

	var wg sync.WaitGroup
	ch := make(chan error)
	numServers := len(listeners)

	wg.Add(numServers)

	go func() {
		wg.Wait()
		close(ch)
	}()

	go func() {
		select {
		case <-ctx.Done():
			logger.Debug("proxy interrupted/killed")
			_ = p.Close()
		}
	}()

	// Serve on all listeners
	for _, listener := range listeners {
		go func(l net.Listener) {
			defer wg.Done()
			// WARNING: Do NOT change this log - the cassandra-bigtable-java-client-lib and compliance tests use the "Starting to serve on listener" log message to check for start up.
			logger.Info(fmt.Sprintf("Starting to serve on listener: %v\n", l.Addr()))
			err := p.Serve(l)
			if err != nil && err != ErrProxyClosed {
				ch <- err
			}
		}(listener)
	}

	for err = range ch {
		if err != nil {
			return err
		}
	}

	return err
}

// resolveAndListen creates and returns a TCP, TLS, or Unix Domain Socket listener
func resolveAndListen(bind string, useUnixSocket bool, unixSocketPath, certFile, keyFile string, logger *zap.Logger) (net.Listener, error) {
	if useUnixSocket {
		// Remove existing socket file if it exists
		if err := os.RemoveAll(unixSocketPath); err != nil {
			return nil, fmt.Errorf("failed to remove existing socket file: %v", err)
		}
		logger.Debug("Creating Unix Domain Socket")
		listener, err := net.Listen("unix", unixSocketPath)
		if err != nil {
			return nil, fmt.Errorf("failed to create Unix Domain Socket: %v", err)
		}
		logger.Debug("Successfully created Unix Domain Socket listener\n")

		// Set socket permissions
		// it is important for the socket permission to stay 0600 (DO NOT CHANGE)
		if err := os.Chmod(unixSocketPath, 0600); err != nil {
			return nil, fmt.Errorf("failed to set socket permissions: %v", err)
		}
		logger.Debug("Set socket permissions\n")

		return listener, nil
	}

	if certFile != "" && keyFile != "" {
		logger.Info(fmt.Sprintf("Setting up TLS listener with cert: %s and key: %s\n", certFile, keyFile))
		cert, err := tls.LoadX509KeyPair(certFile, keyFile)
		if err != nil {
			return nil, fmt.Errorf("unable to load TLS certificate pair: %v", err)
		}
		config := &tls.Config{
			Certificates: []tls.Certificate{cert},
		}
		listener, err := tls.Listen("tcp", bind, config)
		if err != nil {
			return nil, fmt.Errorf("failed to create TLS listener: %v", err)
		}
		logger.Info(fmt.Sprintf("Successfully created TLS listener on %s\n", bind))
		return listener, nil
	}

	logger.Info(fmt.Sprintf("Setting up TCP listener on %s\n", bind))
	listener, err := net.Listen("tcp", bind)

	if err != nil {
		return nil, fmt.Errorf("failed to create TCP listener: %v", err)
	}

	logger.Info("Successfully created TCP listener\n")
	return listener, nil

}

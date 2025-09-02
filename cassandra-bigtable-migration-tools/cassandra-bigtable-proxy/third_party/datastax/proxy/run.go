// Copyright (c) DataStax, Inc.
//
// Licensed under the Apache License, ProtocolVersion 2.0 (the "License");
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
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"net/http"
	"os"
	"sync"

	"github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/global/config"
	"github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/global/constants"
	"github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/utilities"
	"go.uber.org/zap"
)

// Run starts the proxy command. 'args' shouldn't include the executable (i.e. os.Args[1:]). It returns the exit code
// for the proxy.
func Run(ctx context.Context, args []string) error {
	proxyGlobalConfig, proxyInstanceConfigs, err := config.ParseProxyConfigFromArgs(args)
	if err != nil {
		return err
	}

	logger, err := utilities.SetupLogger(proxyGlobalConfig.CliArgs.LogLevel, proxyGlobalConfig.LoggerConfig)
	if err != nil {
		return fmt.Errorf("unable to create logger")
	}
	defer logger.Sync()
	if proxyGlobalConfig.CliArgs.Version {
		fmt.Printf("ProtocolVersion - %s\n", constants.ProxyReleaseVersion)
		return nil
	}

	logger.Info("Protocol Version:" + proxyGlobalConfig.ProtocolVersion.String())
	logger.Info("CQL Version:" + proxyGlobalConfig.CQLVersion)
	logger.Info("Release Version:" + proxyGlobalConfig.ReleaseVersion)
	logger.Info("Partitioner:" + proxyGlobalConfig.Partitioner)
	logger.Info("Data Center:" + proxyGlobalConfig.CliArgs.DataCenter)
	logger.Debug("Configuration - ", zap.Any("ProxyGlobalConfig", proxyGlobalConfig))
	var wg sync.WaitGroup

	for _, listenerConfig := range proxyInstanceConfigs {
		p, err := NewProxy(ctx, listenerConfig)
		if err != nil {
			logger.Error(err.Error())
			return err
		}
		var mux http.ServeMux
		wg.Add(1)
		go func(cfg *config.ProxyGlobalConfig, p *Proxy, mux *http.ServeMux) {
			defer wg.Done()
			err := listenAndServe(listenerConfig, p, mux, ctx, logger) // Use cfg2 or other instances as needed
			if err != nil {
				logger.Fatal("Error while serving - ", zap.Error(err))
			}
		}(proxyGlobalConfig, p, &mux)

	}
	wg.Wait() // Wait for all servers to finish
	logger.Debug("\n>>>>>>>>>>>>> Closed All listeners <<<<<<<<<\n")

	return nil
}

// listenAndServe correctly handles serving both the proxy and an HTTP server simultaneously.
func listenAndServe(c *config.ProxyInstanceConfig, p *Proxy, mux *http.ServeMux, ctx context.Context, logger *zap.Logger) (err error) {
	logger.Info("Starting proxy with configuration:\n")
	logger.Info(fmt.Sprintf("  Bind: %s\n", c.Bind))
	logger.Info(fmt.Sprintf("  Use Unix Socket: %v\n", c.GlobalConfig.CliArgs.UseUnixSocket))
	logger.Info(fmt.Sprintf("  Unix Socket Path: %s\n", c.GlobalConfig.CliArgs.UnixSocketPath))
	logger.Info(fmt.Sprintf("  Use TLS: %v\n", c.GlobalConfig.CliArgs.ProxyCertFile != "" && c.GlobalConfig.CliArgs.ProxyKeyFile != ""))

	var listeners []net.Listener

	// Set up listener based on configuration
	if c.GlobalConfig.CliArgs.UseUnixSocket {
		// Use Unix Domain Socket
		unixListener, err := resolveAndListen("", true, c.GlobalConfig.CliArgs.UnixSocketPath, "", "", logger)
		if err != nil {
			return fmt.Errorf("failed to create Unix socket listener: %v", err)
		}
		listeners = append(listeners, unixListener)
		logger.Info(fmt.Sprintf("Unix socket listener created successfully at %s\n", c.GlobalConfig.CliArgs.UnixSocketPath))
	} else {
		// Use TCP
		tcpListener, err := resolveAndListen(c.GlobalConfig.CliArgs.Bind, false, "", c.GlobalConfig.CliArgs.ProxyCertFile, c.GlobalConfig.CliArgs.ProxyKeyFile, logger)
		if err != nil {
			return fmt.Errorf("failed to create TCP listener: %v", err)
		}
		listeners = append(listeners, tcpListener)
		logger.Info(fmt.Sprintf("TCP listener created successfully on %s\n", c.Bind))
	}

	// Set up BigtableConfig client
	logger.Info("Initializing BigtableConfig client...\n")
	err = p.Connect()
	if err != nil {
		for _, l := range listeners {
			l.Close()
		}
		return err
	}
	logger.Info("BigtableConfig client initialized successfully\n")

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

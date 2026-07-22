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
	"context"
	"errors"
	"fmt"
	"github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/executors"
	"github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/request_handlers"
	"github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/system_tables"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
	"net"
	"sync"

	bigtableModule "github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/bigtable"
	"github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/global/types"
	schemaMapping "github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/metadata"
	otelgo "github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/otel"
	"github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/third_party/datastax/proxycore"
	"github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/translators"
	"github.com/datastax/go-cassandra-native-protocol/message"
	"github.com/datastax/go-cassandra-native-protocol/primitive"
	lru "github.com/hashicorp/golang-lru"
	"go.uber.org/zap"
)

var ErrProxyClosed = errors.New("proxy closed")
var ErrProxyAlreadyConnected = errors.New("proxy already connected")
var ErrProxyNotConnected = errors.New("proxy not connected")

const preparedIdSize = 16
const translatorErrorMessage = "Error occurred at translators"
const errorAtBigtable = "Error occurred at bigtable - "
const errorWhileDecoding = "Error while decoding bytes - "
const unhandledScenario = "Unhandled execution Scenario for prepared CqlQuery"

const (
	traceNamespace = "cassandra.bigtable.proxy"
	handleOptions  = traceNamespace + "/Options"
)

type Server struct {
	ctx                context.Context
	config             *types.ProxyInstanceConfig
	logger             *zap.Logger
	mu                 sync.Mutex
	isConnected        bool
	isClosing          bool
	sessions           map[*clientSession]struct{}
	listeners          map[*net.Listener]struct{}
	eventClients       sync.Map
	preparedQueryCache proxycore.PreparedCache[types.IPreparedQuery]
	closed             chan struct{}
	nodes              []*node
	clientManager      *types.BigtableClientManager
	systemTableManager *system_tables.SystemTableManager
	metadataStore      *schemaMapping.MetadataStore
	bigtableClient     *bigtableModule.BigtableAdapter
	translator         *translators.TranslatorManager
	executor           *executors.QueryExecutorManager
	tracer             trace.Tracer
	otelShutdown       func(context.Context) error
	handlerManager     *request_handlers.HandlerManager
}

func (p *Server) CQLVersion() string {
	return p.config.Options.CQLVersion
}

// HandlePostDDLEvent handles common operations after DDL statements (CREATE, ALTER, DROP)
func (p *Server) HandlePostDDLEvent(queryType types.QueryType, keyspace types.Keyspace, table types.TableName) {
	p.logger.Debug("sending post DDL event", zap.String("queryType", queryType.String()), zap.String("keyspace", string(keyspace)), zap.String("table", string(table)))

	var changeType primitive.SchemaChangeType
	switch queryType {
	case types.QueryTypeCreate:
		changeType = primitive.SchemaChangeTypeCreated
	case types.QueryTypeAlter:
		changeType = primitive.SchemaChangeTypeUpdated
	case types.QueryTypeDrop:
		changeType = primitive.SchemaChangeTypeDropped
	default:
		p.logger.Warn("unhandled ddl event type", zap.String("queryType", queryType.String()))
		return
	}

	// SendEvent all sessions of schema change
	event := &proxycore.SchemaChangeEvent{
		Message: &message.SchemaChangeEvent{
			ChangeType: changeType,
			Target:     primitive.SchemaChangeTargetTable,
			Keyspace:   string(keyspace),
			Object:     string(table),
		},
	}
	p.eventClients.Range(func(key, _ interface{}) bool {
		if session, ok := key.(*clientSession); ok {
			session.handleEvent(event)
		}
		return true
	})
}

type node struct {
	addr   *net.IPAddr
	dc     string
	tokens []string
}

func (p *Server) OnEvent(event proxycore.Event) {
	switch evt := event.(type) {
	case *proxycore.SchemaChangeEvent:
		p.logger.Debug("Schema change event detected", zap.String("SchemaChangeEvent", evt.Message.String()))
	}
}

func NewProxy(ctx context.Context, logger *zap.Logger, config *types.ProxyInstanceConfig) (*Server, error) {
	clientManager, err := types.CreateBigtableClientManager(ctx, config)
	if err != nil {
		return nil, err
	}

	metadataStore := schemaMapping.NewMetadataStore(logger, clientManager, config.BigtableConfig)
	err = metadataStore.Initialize(ctx)
	if err != nil {
		return nil, err
	}

	var shutdownOTel func(context.Context) error
	var otelInst *otelgo.OpenTelemetry
	otelInst, shutdownOTel, err = otelgo.NewOpenTelemetry(ctx, config.OtelConfig, logger)
	if err != nil {
		logger.Error("Failed to enable the OTEL: " + err.Error())
		return nil, err
	}

	bigtableClient := bigtableModule.NewBigtableClient(clientManager, logger, config.BigtableConfig, metadataStore)

	translator := translators.NewTranslatorManager(logger, metadataStore.Schemas(), config.BigtableConfig)

	systemTables := system_tables.NewSystemTableManager(metadataStore, logger)

	handlers := request_handlers.NewHandlerManager()

	proxy := &Server{
		ctx:                ctx,
		config:             config,
		logger:             logger,
		sessions:           make(map[*clientSession]struct{}),
		listeners:          make(map[*net.Listener]struct{}),
		closed:             make(chan struct{}),
		clientManager:      clientManager,
		systemTableManager: systemTables,
		metadataStore:      metadataStore,
		bigtableClient:     bigtableClient,
		translator:         translator,
		executor:           executors.NewQueryExecutorManager(logger, metadataStore.Schemas(), bigtableClient, systemTables.Db(), otelInst),
		tracer:             otel.GetTracerProvider().Tracer("handler"),
		otelShutdown:       shutdownOTel,
		handlerManager:     handlers,
	}

	handlers.InitHandlers(proxy)

	err = systemTables.Initialize(proxy)
	if err != nil {
		logger.Error("Failed to initialize system table manager: " + err.Error())
		return nil, err
	}

	return proxy, nil
}

func (p *Server) Connect() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.isConnected {
		return ErrProxyAlreadyConnected
	}

	var err error
	const cacheSize = 1e8 / 256 // ~100MB with an average query size of 256 bytes
	p.preparedQueryCache, err = NewDefaultPreparedCache[types.IPreparedQuery](cacheSize)
	if err != nil {
		return fmt.Errorf("unable to create cache: %w", err)
	}

	if err != nil {
		return fmt.Errorf("unable to build node information: %w", err)
	}

	err = p.systemTableManager.ReloadSystemTables()
	if err != nil {
		p.logger.Error("failed to update system tables", zap.Error(err))
	}

	p.isConnected = true
	return nil
}

// Serve the proxy using the specified listener. It can be called multiple times with different listeners allowing
// them to share the same backend clusters.
func (p *Server) Serve(l net.Listener) (err error) {
	l = &closeOnceListener{Listener: l}
	defer l.Close()

	if err = p.addListener(&l); err != nil {
		return err
	}
	defer p.removeListener(&l)

	for {
		conn, err := l.Accept()
		if err != nil {
			select {
			case <-p.closed:
				return ErrProxyClosed
			default:
				return err
			}
		}
		p.handle(conn)
	}
}

func (p *Server) GetSystemTableConfig() system_tables.SystemTableConfig {
	var peers []system_tables.PeerConfig
	for _, n := range p.nodes {
		peers = append(peers, system_tables.PeerConfig{
			Addr:   n.addr.String(),
			Dc:     n.dc,
			Tokens: n.tokens,
		})
	}

	systemTableConfig := system_tables.SystemTableConfig{
		RpcAddress:            p.config.RPCAddr,
		Datacenter:            p.config.DC,
		ReleaseVersion:        p.config.Options.ReleaseVersion,
		Partitioner:           p.config.Options.Partitioner,
		CqlVersion:            p.config.Options.CQLVersion,
		NativeProtocolVersion: p.config.Options.ProtocolVersion.String(),
		Peers:                 peers,
	}
	return systemTableConfig
}

func (p *Server) addListener(l *net.Listener) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.isClosing {
		return ErrProxyClosed
	}
	if !p.isConnected {
		return ErrProxyNotConnected
	}
	p.listeners[l] = struct{}{}
	return nil
}

func (p *Server) removeListener(l *net.Listener) {
	p.mu.Lock()
	defer p.mu.Unlock()
	delete(p.listeners, l)
}

func (p *Server) Config() *types.ProxyInstanceConfig {
	return p.config
}

func (p *Server) Logger() *zap.Logger {
	return p.logger
}

func (p *Server) Translator() *translators.TranslatorManager {
	return p.translator
}

func (p *Server) Executor() *executors.QueryExecutorManager {
	return p.executor
}

func (p *Server) PreparedQueryCache() proxycore.PreparedCache[types.IPreparedQuery] {
	return p.preparedQueryCache
}

func (p *Server) BigtableClient() *bigtableModule.BigtableAdapter {
	return p.bigtableClient
}

func (p *Server) EventClients() *sync.Map {
	return &p.eventClients
}

func (p *Server) HandleOptions() string {
	return handleOptions
}

func (p *Server) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	select {
	case <-p.closed:
	default:
		close(p.closed)
	}
	var err error
	for l := range p.listeners {
		if closeErr := (*l).Close(); closeErr != nil && err == nil {
			err = closeErr
		}
	}
	for cl := range p.sessions {
		_ = cl.conn.Close()
		p.eventClients.Delete(cl)
		delete(p.sessions, cl)
	}

	p.clientManager.Close()
	if p.otelShutdown != nil {
		err = p.otelShutdown(p.ctx)
	}
	return err
}

func (p *Server) Ready() bool {
	return true
}

type UCred struct {
	Pid int32
	Uid uint32
}

func getUdsPeerCredentials(conn *net.UnixConn) (UCred, error) {
	return getUdsPeerCredentialsOS(conn)
}

func (p *Server) handle(conn net.Conn) {
	if tcpConn, ok := conn.(*net.TCPConn); ok {
		if err := tcpConn.SetKeepAlive(false); err != nil {
			p.logger.Warn("failed to disable keepalive on connection", zap.Error(err))
		}
		if err := tcpConn.SetNoDelay(true); err != nil {
			p.logger.Warn("failed to set TCP_NODELAY on connection", zap.Error(err))
		}
	}

	if unixConn, ok := conn.(*net.UnixConn); ok {
		UCred, err := getUdsPeerCredentials(unixConn)
		if err != nil || p.config.Options.ClientPid != UCred.Pid || p.config.Options.ClientUid != UCred.Uid {
			_ = conn.Close()
			p.logger.Error("failed to authenticate connection")
		}
	}

	sess := p.createNewSession()
	sess.conn = proxycore.NewConn(conn, sess)
	sess.conn.Start()
}

func (p *Server) createNewSession() *clientSession {
	p.mu.Lock()
	defer p.mu.Unlock()
	cl := newClientSession(p)
	p.sessions[cl] = struct{}{}
	return cl
}

func (p *Server) registerForEvents(cl *clientSession) {
	p.eventClients.Store(cl, struct{}{})
}

func (p *Server) removeClient(cl *clientSession) {
	p.eventClients.Delete(cl)

	p.mu.Lock()
	defer p.mu.Unlock()
	delete(p.sessions, cl)

}

// NewDefaultPreparedCache creates a new default prepared cache capping the max item capacity to `size`.
func NewDefaultPreparedCache[T any](size int) (proxycore.PreparedCache[T], error) {
	cache, err := lru.New(size)
	if err != nil {
		return nil, err
	}
	return &defaultPreparedCache[T]{cache}, nil
}

type defaultPreparedCache[T any] struct {
	cache *lru.Cache
}

func (d defaultPreparedCache[T]) Store(id [16]byte, entry T) {
	d.cache.Add(id, entry)
}

func (d defaultPreparedCache[T]) Load(id [16]byte) (entry T, ok bool) {
	if val, ok := d.cache.Get(id); ok {
		return val.(T), true
	}
	return *new(T), false
}

func preparedIdKey(bytes []byte) [preparedIdSize]byte {
	var buf [preparedIdSize]byte
	copy(buf[:], bytes)
	return buf
}

// Wrap the listener so that if it's closed in the serve loop it doesn't race with proxy Close()
type closeOnceListener struct {
	net.Listener
	once     sync.Once
	closeErr error
}

func (oc *closeOnceListener) Close() error {
	oc.once.Do(oc.close)
	return oc.closeErr
}

func (oc *closeOnceListener) close() { oc.closeErr = oc.Listener.Close() }

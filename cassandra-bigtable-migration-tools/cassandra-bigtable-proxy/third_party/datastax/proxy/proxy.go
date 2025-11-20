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
	"bytes"
	"context"
	"crypto"
	"crypto/md5"
	"errors"
	"fmt"
	"github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/executors"
	"github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/responsehandler"
	cql "github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/third_party/cqlparser"
	"github.com/antlr4-go/antlr/v4"
	"io"
	"net"
	"sync"
	"time"

	"cloud.google.com/go/bigtable"
	bigtableModule "github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/bigtable"
	constants "github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/global/constants"
	"github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/global/types"
	otelgo "github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/otel"
	schemaMapping "github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/schema-mapping"
	"github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/third_party/datastax/parser"
	"github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/third_party/datastax/proxycore"
	"github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/translators"
	"github.com/datastax/go-cassandra-native-protocol/datatype"
	"github.com/datastax/go-cassandra-native-protocol/frame"
	"github.com/datastax/go-cassandra-native-protocol/message"
	"github.com/datastax/go-cassandra-native-protocol/primitive"
	lru "github.com/hashicorp/golang-lru"
	"go.opentelemetry.io/otel/attribute"
	"go.uber.org/zap"
)

var (
	encodedOneValue, _ = proxycore.EncodeType(datatype.Int, primitive.ProtocolVersion4, 1)
)

var ErrProxyClosed = errors.New("proxy closed")
var ErrProxyAlreadyConnected = errors.New("proxy already connected")
var ErrProxyNotConnected = errors.New("proxy not connected")

const systemQueryMetadataNotFoundError = "data not found %s[%v]"

const preparedIdSize = 16
const Query = "CqlQuery"
const BtqlQuery = "BtqlQuery"
const ResultCount = "result count"

const translatorErrorMessage = "Error occurred at translators"
const metadataFetchError = "Error while fetching table Metadata - "
const errorAtBigtable = "Error occurred at bigtable - "
const errorWhileDecoding = "Error while decoding bytes - "
const unhandledScenario = "Unhandled execution Scenario for prepared CqlQuery"
const errQueryNotPrepared = "query is not prepared"
const (
	handleQuery            = "handleQuery"
	handleBatch            = "Batch"
	handleExecuteForDelete = "handleExecuteForDelete"
	handleExecuteForUpdate = "handleExecuteForUpdate"
	handleExecuteForSelect = "handleExecuteForSelect"
	cassandraQuery         = "Cassandra CqlQuery"
	rowKey                 = "Row Key"
)

var (
	systemSchema        = "system_schema"
	keyspaces           = "keyspaces"
	tables              = "tables"
	metaDataColumns     = "metaDataColumns"
	systemVirtualSchema = "system_virtual_schema"
	local               = "local"
)

// Events
const (
	executingBigtableRequestEvent       = "Executing Bigtable Mutation Request"
	executingBigtableSQLAPIRequestEvent = "Executing Bigtable SQL API Request"
	bigtableExecutionDoneEvent          = "bigtable Execution Done"
	gotBulkApplyResp                    = "Got the response for bulk apply"
	sendingBulkApplyMutation            = "Sending Mutation For Bulk Apply"
)

type Proxy struct {
	ctx                      context.Context
	config                   *types.ProxyInstanceConfig
	logger                   *zap.Logger
	cluster                  *proxycore.Cluster
	sessions                 [primitive.ProtocolVersionDse2 + 1]sync.Map // Cache sessions per protocol version
	mu                       sync.Mutex
	isConnected              bool
	isClosing                bool
	clients                  map[*client]struct{}
	listeners                map[*net.Listener]struct{}
	eventClients             sync.Map
	preparedQueryCache       proxycore.PreparedCache[types.IPreparedQuery]
	queryMetadataCache       proxycore.PreparedCache[*message.PreparedResult]
	systemLocalValues        map[string]message.Column
	closed                   chan struct{}
	localNode                *node
	nodes                    []*node
	bClient                  bigtableModule.BigTableClientIface
	translator               *translators.TranslatorManager
	executor                 *executors.QueryExecutorManager
	schemaMapping            *schemaMapping.SchemaMappingConfig
	otelInst                 *otelgo.OpenTelemetry
	otelShutdown             func(context.Context) error
	systemQueryMetadataCache *SystemQueryMetadataCache
}

type node struct {
	addr   *net.IPAddr
	dc     string
	tokens []string
}

func (p *Proxy) OnEvent(event proxycore.Event) {
	switch evt := event.(type) {
	case *proxycore.SchemaChangeEvent:
		p.logger.Debug("Schema change event detected", zap.String("SchemaChangeEvent", evt.Message.String()))
	}
}

func createBigtableConnection(ctx context.Context, config *types.ProxyInstanceConfig) (map[string]*bigtable.Client, map[string]*bigtable.AdminClient, error) {
	bigtableClients, adminClients, err := bigtableModule.CreateClientsForInstances(ctx, config)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create bigtable client: %w", err)
	}
	return bigtableClients, adminClients, nil
}

func NewProxy(ctx context.Context, logger *zap.Logger, config *types.ProxyInstanceConfig) (*Proxy, error) {
	bigtableClients, adminClients, err := createBigtableConnection(ctx, config)
	if err != nil {
		return nil, err
	}
	smc := schemaMapping.NewSchemaMappingConfig(types.TableName(config.BigtableConfig.SchemaMappingTable), config.BigtableConfig.DefaultColumnFamily, logger, nil)
	bigtableCl := bigtableModule.NewBigtableClient(bigtableClients, adminClients, logger, config.BigtableConfig, smc)
	for k := range config.BigtableConfig.Instances {
		tableConfigs, err := bigtableCl.ReadTableConfigs(ctx, k)
		if err != nil {
			return nil, err
		}
		smc.UpdateTables(tableConfigs)
	}
	bigtableCl.LoadConfigs(smc)
	translator := translators.NewTranslatorManager(logger, smc, config.BigtableConfig.DefaultIntRowKeyEncoding)

	// Enable OpenTelemetry traces by setting environment variable GOOGLE_API_GO_EXPERIMENTAL_TELEMETRY_PLATFORM_TRACING to the case-insensitive value "opentelemetry" before loading the client library.
	otelConfig := &otelgo.OTelConfig{}
	otelInst := &otelgo.OpenTelemetry{Config: &otelgo.OTelConfig{OTELEnabled: false}}

	var shutdownOTel func(context.Context) error
	// Initialize OpenTelemetry
	if config.OtelConfig.Enabled {
		otelConfig = &otelgo.OTelConfig{
			TracerEndpoint:     config.OtelConfig.Traces.Endpoint,
			MetricEndpoint:     config.OtelConfig.Metrics.Endpoint,
			ServiceName:        config.OtelConfig.ServiceName,
			OTELEnabled:        config.OtelConfig.Enabled,
			TraceSampleRatio:   config.OtelConfig.Traces.SamplingRatio,
			HealthCheckEnabled: config.OtelConfig.HealthCheck.Enabled,
			HealthCheckEp:      config.OtelConfig.HealthCheck.Endpoint,
			ServiceVersion:     config.Options.ProtocolVersion.String(),
		}
	} else {
		otelConfig = &otelgo.OTelConfig{OTELEnabled: false}
	}
	otelInst, shutdownOTel, err = otelgo.NewOpenTelemetry(ctx, otelConfig, logger)
	if err != nil {
		logger.Error("Failed to enable the OTEL: " + err.Error())
		return nil, err
	}
	systemQueryMetadataCache, err := ConstructSystemMetadataRows(smc.GetAllTables())
	if err != nil {
		return nil, err
	}

	proxy := Proxy{
		ctx:                      ctx,
		config:                   config,
		logger:                   logger,
		clients:                  make(map[*client]struct{}),
		listeners:                make(map[*net.Listener]struct{}),
		closed:                   make(chan struct{}),
		bClient:                  bigtableCl,
		translator:               translator,
		executor:                 executors.NewQueryExecutorManager(smc, bigtableCl),
		schemaMapping:            smc,
		systemQueryMetadataCache: systemQueryMetadataCache,
		otelInst:                 otelInst,
		otelShutdown:             shutdownOTel,
	}
	return &proxy, nil
}

func (p *Proxy) Connect() error {
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
	p.queryMetadataCache, err = NewDefaultPreparedCache[*message.PreparedResult](cacheSize)
	if err != nil {
		return fmt.Errorf("unable to create cache: %w", err)
	}

	//  connecting to cassandra cluster
	p.cluster, err = proxycore.ConnectCluster(p.ctx, proxycore.ClusterConfig{
		Version: p.config.Options.ProtocolVersion,
		Logger:  p.logger,
	})

	if err != nil {
		return fmt.Errorf("unable to connect to cluster %w", err)
	}

	err = p.buildNodes()
	if err != nil {
		return fmt.Errorf("unable to build node information: %w", err)
	}

	p.buildLocalRow()

	// Create cassandra session
	sess, err := proxycore.ConnectSession(p.ctx, p.cluster, proxycore.SessionConfig{
		Version: p.cluster.NegotiatedVersion,
		Logger:  p.logger,
	})

	if err != nil {
		return fmt.Errorf("unable to connect session %w", err)
	}

	p.sessions[p.cluster.NegotiatedVersion].Store("", sess)

	p.isConnected = true
	return nil
}

// Serve the proxy using the specified listener. It can be called multiple times with different listeners allowing
// them to share the same backend clusters.
func (p *Proxy) Serve(l net.Listener) (err error) {
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

func (p *Proxy) addListener(l *net.Listener) error {
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

func (p *Proxy) removeListener(l *net.Listener) {
	p.mu.Lock()
	defer p.mu.Unlock()
	delete(p.listeners, l)
}

func (p *Proxy) Close() error {
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
	for cl := range p.clients {
		_ = cl.conn.Close()
		p.eventClients.Delete(cl)
		delete(p.clients, cl)
	}

	p.bClient.Close()
	if p.otelShutdown != nil {
		err = p.otelShutdown(p.ctx)
	}
	return err
}

func (p *Proxy) Ready() bool {
	return true
}

type UCred struct {
	Pid int32
	Uid uint32
}

func getUdsPeerCredentials(conn *net.UnixConn) (UCred, error) {
	return getUdsPeerCredentialsOS(conn)
}

func (p *Proxy) handle(conn net.Conn) {
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
			conn.Close()
			p.logger.Error("failed to authenticate connection")
		}
	}

	cl := &client{
		ctx:   p.ctx,
		proxy: p,
	}
	cl.sender = cl
	p.addClient(cl)
	cl.conn = proxycore.NewConn(conn, cl)
	cl.conn.Start()
}

var (
	schemaVersion, _ = primitive.ParseUuid("4f2b29e6-59b5-4e2d-8fd6-01e32e67f0d7")
)

func (p *Proxy) buildNodes() (err error) {

	localDC := p.config.DC
	if len(localDC) == 0 {
		localDC = p.cluster.Info.LocalDC
		p.logger.Info("no local DC configured using DC from the first successful contact point",
			zap.String("dc", localDC))
	}

	p.localNode = &node{
		dc: localDC,
	}

	return nil
}

func (p *Proxy) buildLocalRow() {
	p.systemLocalValues = map[string]message.Column{
		"key":                     p.encodeTypeFatal(datatype.Varchar, "local"),
		"data_center":             p.encodeTypeFatal(datatype.Varchar, p.localNode.dc),
		"rack":                    p.encodeTypeFatal(datatype.Varchar, "rack1"),
		"tokens":                  p.encodeTypeFatal(datatype.NewListType(datatype.Varchar), [1]string{"-9223372036854775808"}),
		"release_version":         p.encodeTypeFatal(datatype.Varchar, p.config.Options.ReleaseVersion),
		"partitioner":             p.encodeTypeFatal(datatype.Varchar, p.config.Options.Partitioner),
		"cluster_name":            p.encodeTypeFatal(datatype.Varchar, fmt.Sprintf("cassandra-bigtable-proxy-%s", constants.ProxyReleaseVersion)),
		"cql_version":             p.encodeTypeFatal(datatype.Varchar, p.config.Options.CQLVersion),
		"schema_version":          p.encodeTypeFatal(datatype.Uuid, schemaVersion), // TODO: Make this match the downstream cluster(s)
		"native_protocol_version": p.encodeTypeFatal(datatype.Varchar, p.config.Options.ProtocolVersion.String()),
		"dse_version":             p.encodeTypeFatal(datatype.Varchar, p.cluster.Info.DSEVersion),
	}
}

func (p *Proxy) encodeTypeFatal(dt datatype.DataType, val interface{}) []byte {
	encoded, err := proxycore.EncodeType(dt, p.config.Options.ProtocolVersion, val)
	if err != nil {
		p.logger.Fatal("unable to encode type", zap.Error(err))
	}
	return encoded
}

func (p *Proxy) addClient(cl *client) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.clients[cl] = struct{}{}
}

func (p *Proxy) registerForEvents(cl *client) {
	p.eventClients.Store(cl, struct{}{})
}

func (p *Proxy) removeClient(cl *client) {
	p.eventClients.Delete(cl)

	p.mu.Lock()
	defer p.mu.Unlock()
	delete(p.clients, cl)

}

type Sender interface {
	Send(hdr *frame.Header, msg message.Message)
}

type client struct {
	sessionKeyspace types.Keyspace
	ctx             context.Context
	proxy           *Proxy
	conn            *proxycore.Conn
	sender          Sender
}

func (c *client) SetSessionKeyspace(k types.Keyspace) {
	c.sessionKeyspace = k
}

func (c *client) Receive(reader io.Reader) error {
	raw, err := codec.DecodeRawFrame(reader)
	if err != nil {
		if !errors.Is(err, io.EOF) {
			c.proxy.logger.Error("unable to decode frame", zap.Error(err))
		}
		return err
	}

	if raw.Header.Version > c.proxy.config.Options.MaxProtocolVersion || raw.Header.Version < primitive.ProtocolVersion3 {
		c.sender.Send(raw.Header, &message.ProtocolError{
			ErrorMessage: fmt.Sprintf("Invalid or unsupported protocol version %d", raw.Header.Version),
		})
		return nil
	}

	body, err := codec.DecodeBody(raw.Header, bytes.NewReader(raw.Body))
	if err != nil {
		c.proxy.logger.Error("unable to decode body", zap.Error(err))
		return err
	}

	switch msg := body.Message.(type) {
	case *message.Options:
		// CC - responding with status READY
		c.sender.Send(raw.Header, &message.Supported{Options: map[string][]string{
			"CQL_VERSION": {c.proxy.config.Options.CQLVersion},
			"COMPRESSION": {},
		}})
	case *message.Startup:
		// CC -  register for Event types and respond READY
		c.sender.Send(raw.Header, &message.Ready{})
	case *message.Register:
		c.proxy.logger.Info("Client registered for events", zap.Any("event_types", msg.EventTypes))
		for _, t := range msg.EventTypes {
			if t == primitive.EventTypeSchemaChange {
				c.proxy.registerForEvents(c)
			}
		}
		c.sender.Send(raw.Header, &message.Ready{})
	case *message.Prepare:
		c.handlePrepare(raw, msg)
	case *partialExecute:
		c.handleExecute(raw, msg)
	case *partialQuery:
		c.handleQuery(raw, msg)
	case *partialBatch:
		c.handleBatch(raw, msg)
	default:
		c.sender.Send(raw.Header, &message.ProtocolError{ErrorMessage: "Unsupported operation"})
	}
	return nil
}

func (c *client) handlePrepare(raw *frame.RawFrame, msg *message.Prepare) {
	id := c.getQueryId(msg)
	if response, found := c.proxy.queryMetadataCache.Load(id); found {
		c.sender.Send(raw.Header, response)
		return
	}

	c.proxy.logger.Debug("preparing query", zap.String(Query, msg.Query), zap.Int16("stream", raw.Header.StreamId))

	keyspace := c.sessionKeyspace
	if len(msg.Keyspace) != 0 {
		keyspace = types.Keyspace(msg.Keyspace)
	}

	p := newParser(msg.Query)
	qt, err := parseQueryType(p)
	if err != nil {
		c.proxy.logger.Error("error parsing query to see if it's handled", zap.String(Query, msg.Query), zap.Error(err))
		c.sender.Send(raw.Header, &message.Invalid{ErrorMessage: err.Error()})
		return
	}
	rawQuery := types.NewRawQuery(raw.Header, keyspace, msg.Query, p, qt)

	c.handleServerPreparedQuery(rawQuery, id)
}

func parseQueryType(p *cql.CqlParser) (types.QueryType, error) {
	tok := p.GetTokenStream().LT(1)
	t := tok.GetTokenType()
	switch t {
	case cql.CqlLexerK_SELECT:
		return types.QueryTypeSelect, nil
	case cql.CqlLexerK_INSERT:
		return types.QueryTypeInsert, nil
	case cql.CqlLexerK_UPDATE:
		return types.QueryTypeUpdate, nil
	case cql.CqlLexerK_DELETE:
		return types.QueryTypeDelete, nil
	case cql.CqlLexerK_CREATE:
		return types.QueryTypeCreate, nil
	case cql.CqlLexerK_ALTER:
		return types.QueryTypeAlter, nil
	case cql.CqlLexerK_DROP:
		return types.QueryTypeDrop, nil
	case cql.CqlLexerK_TRUNCATE:
		return types.QueryTypeTruncate, nil
	case cql.CqlLexerK_USE:
		return types.QueryTypeUse, nil
	case cql.CqlLexerK_DESCRIBE, cql.CqlLexerK_DESC:
		return types.QueryTypeDescribe, nil
	default:
		return types.QueryTypeUnknown, fmt.Errorf("unsupported query type: %s", tok.String())
	}
}

func (c *client) getQueryId(msg *message.Prepare) [16]byte {
	// Generating unique prepared query_id
	return md5.Sum([]byte(msg.Query + string(c.sessionKeyspace)))
}

func newParser(query string) *cql.CqlParser {
	lexer := cql.NewCqlLexer(antlr.NewInputStream(query))
	stream := antlr.NewCommonTokenStream(lexer, antlr.TokenDefaultChannel)
	return cql.NewCqlParser(stream)
}

// handleServerPreparedQuery handle prepared query that was supposed to run on cassandra server
// This method will keep track of prepared query in a map and send hashed query_id with result
// metadata and variable column metadata to the client
//
// Parameters:
//   - raw: *frame.RawFrame
//   - msg: *message.Prepare
//
// Returns: nil
func (c *client) handleServerPreparedQuery(query *types.RawQuery, id [16]byte) {
	preparedQuery, _, err := c.proxy.translator.TranslateQuery(query, c.sessionKeyspace, true)
	if err != nil {
		c.proxy.logger.Error(translatorErrorMessage, zap.String(Query, query.RawCql()), zap.Error(err))
		c.sender.Send(query.Header(), &message.Invalid{ErrorMessage: err.Error()})
		return
	}

	btPreparedQuery, err := c.proxy.bClient.PrepareStatement(c.ctx, preparedQuery)
	if err != nil {
		c.proxy.logger.Error(translatorErrorMessage, zap.String(Query, query.RawCql()), zap.Error(err))
		c.sender.Send(query.Header(), &message.Invalid{ErrorMessage: err.Error()})
		return
	}
	preparedQuery.SetBigtablePreparedQuery(btPreparedQuery)

	response, err := responsehandler.BuildPreparedResultResponse(id, preparedQuery.Keyspace(), preparedQuery.Table(), preparedQuery.Parameters(), preparedQuery.ResponseColumns())

	// update caches
	c.proxy.preparedQueryCache.Store(id, preparedQuery)
	c.proxy.queryMetadataCache.Store(id, response)

	c.sender.Send(query.Header(), response)
}

// handleExecute for prepared query
func (c *client) handleExecute(raw *frame.RawFrame, msg *partialExecute) {
	ctx := context.Background()
	id := preparedIdKey(msg.queryId)

	preparedStmt, ok := c.proxy.preparedQueryCache.Load(id)
	if !ok {
		c.proxy.logger.Error(errQueryNotPrepared)
		c.sender.Send(raw.Header, &message.ServerError{ErrorMessage: errQueryNotPrepared})
		return
	}

	boundQuery, err := c.proxy.translator.BindQuery(preparedStmt, msg.PositionalValues, raw.Header.Version)
	if err != nil {
		c.proxy.logger.Error(errorWhileDecoding, zap.String(Query, preparedStmt.CqlQuery()), zap.Error(err))
		c.sender.Send(raw.Header, &message.ConfigError{ErrorMessage: err.Error()})
		return
	}

	results, err := c.proxy.executor.Execute(ctx, c, boundQuery)
	if err != nil {
		c.proxy.logger.Error(errorAtBigtable, zap.String(Query, preparedStmt.CqlQuery()), zap.String(Query, preparedStmt.BigtableQuery()), zap.Error(err))
		c.sender.Send(raw.Header, &message.ConfigError{ErrorMessage: err.Error()})
		return
	}
	c.sender.Send(raw.Header, results)
}

// handle batch queries
func (c *client) handleBatch(raw *frame.RawFrame, msg *partialBatch) {
	startTime := time.Now()
	otelCtx, span := c.proxy.otelInst.StartSpan(c.proxy.ctx, handleBatch, []attribute.KeyValue{
		attribute.Int("Batch Size", len(msg.queryOrIds)),
	})
	defer c.proxy.otelInst.EndSpan(span)
	var otelErr error
	defer c.proxy.otelInst.RecordMetrics(otelCtx, handleBatch, startTime, handleBatch, c.sessionKeyspace, otelErr)
	bulkMutations, keyspace, err := c.bindBulkOperations(msg, raw.Header.Version)
	if err != nil {
		c.proxy.logger.Error("Error preparing batch query metadata", zap.Error(err))
		c.sender.Send(raw.Header, &message.ConfigError{ErrorMessage: err.Error()})
		otelErr = err
		c.proxy.otelInst.RecordError(span, otelErr)
		return
	}
	otelgo.AddAnnotation(otelCtx, sendingBulkApplyMutation)
	var errorMessage string
	hasError := false
	for tableName, mutations := range bulkMutations.Mutations() {
		res, err := c.proxy.bClient.ApplyBulkMutation(otelCtx, keyspace, tableName, mutations)
		if err != nil || res.FailedRows != "" {
			c.proxy.otelInst.RecordError(span, err)
			hasError = true
			errorMessage = errorMessage + res.FailedRows + "\n"
		}
	}
	otelgo.AddAnnotation(otelCtx, gotBulkApplyResp)
	if hasError {
		c.sender.Send(raw.Header, &message.ServerError{ErrorMessage: errorMessage})
	}
	c.sender.Send(raw.Header, &message.VoidResult{})
}

func (c *client) bindBulkOperations(msg *partialBatch, pv primitive.ProtocolVersion) (*bigtableModule.BigtableBulkMutation, types.Keyspace, error) {
	var keyspace types.Keyspace
	tableMutationsMap := bigtableModule.NewBigtableBulkMutation()
	for index, queryId := range msg.queryOrIds {
		queryOrId, ok := queryId.([]byte)
		if !ok {
			return nil, "", fmt.Errorf("batch query id malformed")
		}
		id := preparedIdKey(queryOrId)
		preparedStmt, ok := c.proxy.preparedQueryCache.Load(id)
		if !ok {
			return nil, "", fmt.Errorf("prepared query not found in cache")
		}

		executableQuery, err := c.proxy.translator.BindQuery(preparedStmt, msg.BatchPositionalValues[index], pv)
		if err != nil {
			return nil, "", err
		}
		mutation, ok := executableQuery.AsBulkMutation()
		if !ok {
			return nil, "", fmt.Errorf("query type '%s' not compatible with bulk", executableQuery.QueryType().String())
		}
		tableMutationsMap.AddMutation(mutation)
	}
	return tableMutationsMap, keyspace, nil
}

func (c *client) handleQuery(raw *frame.RawFrame, msg *partialQuery) {
	startTime := time.Now()
	c.proxy.logger.Debug("handling query", zap.String("encodedQuery", msg.query), zap.Int16("stream", raw.Header.StreamId))
	otelCtx, span := c.proxy.otelInst.StartSpan(c.proxy.ctx, handleQuery, []attribute.KeyValue{
		attribute.String("CqlQuery", msg.query),
	})

	p := newParser(msg.query)
	qt, err := parseQueryType(p)
	if err != nil {
		c.proxy.logger.Error("error parsing query to see if it's handled", zap.String(Query, msg.query), zap.Error(err))
		c.sender.Send(raw.Header, &message.Invalid{ErrorMessage: err.Error()})
		return
	}

	rawQuery := types.NewRawQuery(raw.Header, c.sessionKeyspace, msg.query, p, qt)
	defer c.proxy.otelInst.EndSpan(span)

	var otelErr error
	defer c.proxy.otelInst.RecordMetrics(otelCtx, handleQuery, startTime, rawQuery.QueryType().String(), c.sessionKeyspace, otelErr)

	_, bound, err := c.proxy.translator.TranslateQuery(rawQuery, c.sessionKeyspace, false)
	if err != nil {
		c.proxy.logger.Error(translatorErrorMessage, zap.String(Query, msg.query), zap.Error(err))
		otelErr = err
		c.proxy.otelInst.RecordError(span, otelErr)
		c.sender.Send(raw.Header, &message.Invalid{ErrorMessage: err.Error()})
		return
	}

	otelgo.AddAnnotation(otelCtx, executingBigtableSQLAPIRequestEvent)
	selectResult, err := c.proxy.executor.Execute(otelCtx, c, bound)
	otelgo.AddAnnotation(otelCtx, bigtableExecutionDoneEvent)

	if err != nil {
		c.proxy.logger.Error(errorAtBigtable, zap.String(Query, msg.query), zap.Error(err))
		otelErr = err
		c.proxy.otelInst.RecordError(span, otelErr)
		c.sender.Send(raw.Header, &message.Invalid{ErrorMessage: err.Error()})
		return
	}

	c.sender.Send(raw.Header, selectResult)
}

func (c *client) filterSystemLocalValues(stmt *parser.SelectStatement, filtered []*message.ColumnMetadata) (row []message.Column, err error) {
	return parser.FilterValues(stmt, filtered, func(name string) (value message.Column, err error) {
		if name == "rpc_address" {
			return proxycore.EncodeType(datatype.Inet, c.proxy.cluster.NegotiatedVersion, c.localIP())
		} else if name == "host_id" {
			return proxycore.EncodeType(datatype.Uuid, c.proxy.cluster.NegotiatedVersion, nameBasedUUID(c.localIP().String()))
		} else if val, ok := c.proxy.systemLocalValues[name]; ok {
			return val, nil
		} else if name == parser.CountValueName {
			return encodedOneValue, nil
		} else {
			return nil, fmt.Errorf("no column value for %s", name)
		}
	})
}

func (c *client) localIP() net.IP {
	if c.proxy.config.RPCAddr != "" {
		return net.ParseIP(c.proxy.config.RPCAddr)
	}
	return net.ParseIP("127.0.0.1")
}

func (c *client) filterSystemPeerValues(stmt *parser.SelectStatement, filtered []*message.ColumnMetadata, peer *node, peerCount int) (row []message.Column, err error) {
	return parser.FilterValues(stmt, filtered, func(name string) (value message.Column, err error) {
		if name == "data_center" {
			return proxycore.EncodeType(datatype.Varchar, c.proxy.cluster.NegotiatedVersion, peer.dc)
		} else if name == "host_id" {
			return proxycore.EncodeType(datatype.Uuid, c.proxy.cluster.NegotiatedVersion, nameBasedUUID(peer.addr.String()))
		} else if name == "tokens" {
			return proxycore.EncodeType(datatype.NewListType(datatype.Varchar), c.proxy.cluster.NegotiatedVersion, peer.tokens)
		} else if name == "peer" {
			return proxycore.EncodeType(datatype.Inet, c.proxy.cluster.NegotiatedVersion, peer.addr.IP)
		} else if name == "rpc_address" {
			return proxycore.EncodeType(datatype.Inet, c.proxy.cluster.NegotiatedVersion, peer.addr.IP)
		} else if val, ok := c.proxy.systemLocalValues[name]; ok {
			return val, nil
		} else if name == parser.CountValueName {
			return proxycore.EncodeType(datatype.Int, c.proxy.cluster.NegotiatedVersion, peerCount)
		} else {
			return nil, fmt.Errorf("no column value for %s", name)
		}
	})
}

// getSystemMetadata retrieves system metadata for `system_schema` keyspaces, tables, or columns.
//
// Parameters:
// - hdr: *frame.Header (request version info)
// - s: *parser.SelectStatement (sessionKeyspace and table info)
//
// Returns:
// - []message.Row: Metadata rows for the requested table; empty if sessionKeyspace/table is invalid.
func (c *client) getSystemMetadata(hdr *frame.Header, s *parser.SelectStatement) ([]message.Row, error) {
	if s.Keyspace != systemSchema || (s.Table != keyspaces && s.Table != tables && s.Table != metaDataColumns) {
		return nil, nil
	}

	var cache map[primitive.ProtocolVersion][]message.Row
	var errMsg error
	switch s.Table {
	case keyspaces:
		cache = c.proxy.systemQueryMetadataCache.KeyspaceSystemQueryMetadataCache
		errMsg = fmt.Errorf(systemQueryMetadataNotFoundError, "KeyspaceSystemQueryMetadataCache", hdr.Version)
	case tables:
		cache = c.proxy.systemQueryMetadataCache.TableSystemQueryMetadataCache
		errMsg = fmt.Errorf(systemQueryMetadataNotFoundError, "TableSystemQueryMetadataCache", hdr.Version)
	case metaDataColumns:
		cache = c.proxy.systemQueryMetadataCache.ColumnsSystemQueryMetadataCache
		errMsg = fmt.Errorf(systemQueryMetadataNotFoundError, "ColumnsSystemQueryMetadataCache", hdr.Version)
	}

	if data, exist := cache[hdr.Version]; !exist {
		return nil, errMsg
	} else {
		return data, nil
	}
}

func (c *client) Send(hdr *frame.Header, msg message.Message) {
	_ = c.conn.Write(proxycore.SenderFunc(func(writer io.Writer) error {
		return codec.EncodeFrame(frame.NewFrame(hdr.Version, hdr.StreamId, msg), writer)
	}))
}

func (c *client) Closing(_ error) {
	c.proxy.removeClient(c)
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

func nameBasedUUID(name string) primitive.UUID {
	var uuid primitive.UUID
	m := crypto.MD5.New()
	_, _ = io.WriteString(m, name)
	hash := m.Sum(nil)
	for i := 0; i < len(uuid); i++ {
		uuid[i] = hash[i]
	}
	uuid[6] &= 0x0F
	uuid[6] |= 0x30
	uuid[8] &= 0x3F
	uuid[8] |= 0x80
	return uuid
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

// handleEvent handles events from the proxy core
// It sends the event message to all connected clients.
func (c *client) handleEvent(event proxycore.Event) {
	switch evt := event.(type) {
	case *proxycore.SchemaChangeEvent:
		c.sender.Send(&frame.Header{
			Version:  c.proxy.config.Options.ProtocolVersion,
			StreamId: -1, // -1 for events
			OpCode:   primitive.OpCodeEvent,
		}, evt.Message)
	}
}

// handlePostDDLEvent handles common operations after DDL statements (CREATE, ALTER, DROP)
func (c *client) handlePostDDLEvent(hdr *frame.Header, changeType primitive.SchemaChangeType, keyspace, table string) {
	// Refresh system metadata cache
	cache, err := ConstructSystemMetadataRows(c.proxy.schemaMapping.GetAllTables())
	if err != nil {
		c.proxy.logger.Error("Failed to refresh system metadata cache", zap.Error(err))
		_, span := c.proxy.otelInst.StartSpan(c.proxy.ctx, "handlePostDDLEvent", nil)
		c.proxy.otelInst.RecordError(span, err)
		c.proxy.otelInst.EndSpan(span)
		c.sender.Send(hdr, &message.Invalid{ErrorMessage: err.Error()})
	}
	c.proxy.systemQueryMetadataCache = cache

	// Notify all clients of schema change
	event := &proxycore.SchemaChangeEvent{
		Message: &message.SchemaChangeEvent{
			ChangeType: changeType,
			Target:     primitive.SchemaChangeTargetTable,
			Keyspace:   keyspace,
			Object:     table,
		},
	}
	c.proxy.eventClients.Range(func(key, _ interface{}) bool {
		if client, ok := key.(*client); ok {
			client.handleEvent(event)
		}
		return true
	})
}

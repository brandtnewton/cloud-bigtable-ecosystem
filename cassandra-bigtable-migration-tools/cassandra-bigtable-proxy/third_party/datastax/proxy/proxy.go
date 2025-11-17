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
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"reflect"
	"sort"
	"strings"
	"sync"
	"time"

	"cloud.google.com/go/bigtable"
	bigtableModule "github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/bigtable"
	constants "github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/global/constants"
	"github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/global/types"
	otelgo "github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/otel"
	"github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/responsehandler"
	schemaMapping "github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/schema-mapping"
	"github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/third_party/datastax/parser"
	"github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/third_party/datastax/proxycore"
	"github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/translator"
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

const selectType = "select"
const describeType = "describe"
const alterType = "alter"
const updateType = "update"
const insertType = "insert"
const deleteType = "delete"
const createType = "create"
const truncateType = "truncate"
const dropType = "drop"

const ts_column types.ColumnName = "last_commit_ts"
const preparedIdSize = 16
const Query = "CqlQuery"

const translatorErrorMessage = "Error occurred at translator"
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
	preparedCache            proxycore.PreparedCache
	preparedIdempotence      sync.Map
	systemLocalValues        map[string]message.Column
	closed                   chan struct{}
	localNode                *node
	nodes                    []*node
	bClient                  bigtableModule.BigTableClientIface
	translator               *translator.Translator
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
	proxyTranslator := &translator.Translator{
		Logger:                   logger,
		SchemaMappingConfig:      smc,
		DefaultIntRowKeyEncoding: config.BigtableConfig.DefaultIntRowKeyEncoding,
	}

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
		translator:               proxyTranslator,
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
	p.preparedCache, err = NewDefaultPreparedCache(1e8 / 256) // ~100MB with an average query size of 256 bytes
	if err != nil {
		return fmt.Errorf("unable to create prepared cache %w", err)
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
		Version:       p.cluster.NegotiatedVersion,
		PreparedCache: p.preparedCache,
		Logger:        p.logger,
	})

	if err != nil {
		return fmt.Errorf("unable to connect session %w", err)
	}

	p.sessions[p.cluster.NegotiatedVersion].Store("", sess) // No keyspace

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
		ctx:                 p.ctx,
		proxy:               p,
		preparedSystemQuery: newProxyCache[any](),
		preparedQueries:     newProxyCache[translator.IPreparedQuery](),
		queryMetadataCache:  newProxyCache[message.PreparedResult](),
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

// isIdempotent checks whether a prepared ID is idempotent.
// If the proxy receives a query that it's never prepared then this will also return false.
func (p *Proxy) IsIdempotent(id []byte) bool {
	if val, ok := p.preparedIdempotence.Load(preparedIdKey(id)); !ok {
		// This should only happen if the proxy has never had a "PREPARE" request for this query ID.
		p.logger.Error("unable to determine if prepared statement is idempotent",
			zap.String("preparedID", hex.EncodeToString(id)))
		return false
	} else {
		return val.(bool)
	}
}

// MaybeStorePreparedIdempotence stores the idempotence of a "PREPARE" request's query.
// This information is used by future "EXECUTE" requests when they need to be retried.
func (p *Proxy) MaybeStorePreparedIdempotence(raw *frame.RawFrame, msg message.Message) {
	if prepareMsg, ok := msg.(*message.Prepare); ok && raw.Header.OpCode == primitive.OpCodeResult { // Prepared result
		frm, err := codec.ConvertFromRawFrame(raw)
		if err != nil {
			p.logger.Error("error attempting to decode prepared result message")
		} else if _, ok = frm.Body.Message.(*message.PreparedResult); !ok { // TODO: Use prepared type data to disambiguate idempotency
			p.logger.Error("expected prepared result message, but got something else")
		} else {
			idempotent, err := parser.IsQueryIdempotent(prepareMsg.Query)
			if err != nil {
				p.logger.Error("error parsing query for idempotence", zap.Error(err))
			} else if result, ok := frm.Body.Message.(*message.PreparedResult); ok {
				p.preparedIdempotence.Store(preparedIdKey(result.PreparedQueryId), idempotent)
			} else {
				p.logger.Error("expected prepared result, but got some other type of message",
					zap.Stringer("type", reflect.TypeOf(frm.Body.Message)))
			}
		}
	}
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
	ctx                 context.Context
	proxy               *Proxy
	conn                *proxycore.Conn
	keyspace            types.Keyspace
	preparedSystemQuery *proxyCache[any]
	preparedQueries     *proxyCache[translator.IPreparedQuery]
	queryMetadataCache  *proxyCache[*message.PreparedResult]
	sender              Sender
}

type PreparedQuery struct {
	Query           string
	SelectedColumns []string
	PreparedColumns []string
}

type UsePreparedQuery struct {
	Query string
	// keyspace string
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
		c.proxy.logger.Debug("Prepare block -", zap.String(Query, msg.Query))
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

// function to execute query on cassandra
func (c *client) handlePrepare(raw *frame.RawFrame, msg *message.Prepare) {
	c.proxy.logger.Debug("handling prepare", zap.String(Query, msg.Query), zap.Int16("stream", raw.Header.StreamId))

	keyspace := c.keyspace
	if len(msg.Keyspace) != 0 {
		keyspace = types.Keyspace(msg.Keyspace)
	}

	handled, stmt, queryType, err := parser.IsQueryHandledWithQueryType(parser.IdentifierFromString(string(keyspace)), msg.Query)
	if err != nil {
		c.proxy.logger.Error("error parsing query to see if it's handled", zap.String(Query, msg.Query), zap.Error(err))
		c.sender.Send(raw.Header, &message.Invalid{ErrorMessage: err.Error()})
		return
	}
	if handled {
		switch s := stmt.(type) {
		// handling select statement
		case *parser.SelectStatement:
			if systemColumns, ok := parser.SystemColumnsByName[s.Table]; ok {
				if columns, err := parser.FilterColumns(s, systemColumns); err != nil {
					c.sender.Send(raw.Header, &message.Invalid{ErrorMessage: err.Error()})
				} else {
					id := c.getQueryId(msg)
					c.sender.Send(raw.Header, &message.PreparedResult{
						PreparedQueryId: id[:],
						ResultMetadata: &message.RowsMetadata{
							ColumnCount: int32(len(columns)),
							Columns:     columns,
						},
					})
					c.preparedSystemQuery.set(id, stmt)
				}
			} else {
				c.sender.Send(raw.Header, &message.Invalid{ErrorMessage: "system metaDataColumns doesn't exist"})
			}
			// Prepare Use statement
		case *parser.UseStatement:
			id := md5.Sum([]byte(msg.Query))
			c.preparedSystemQuery.set(id, stmt)
			c.sender.Send(raw.Header, &message.PreparedResult{
				PreparedQueryId: id[:],
			})
		default:
			c.sender.Send(raw.Header, &message.ServerError{ErrorMessage: "Proxy attempted to intercept an unhandled query"})
		}
	} else {
		c.handleServerPreparedQuery(raw, msg, queryType)
	}
}

func (c *client) getQueryId(msg *message.Prepare) [16]byte {
	// Generating unique prepared query_id
	return md5.Sum([]byte(msg.Query + string(c.keyspace)))
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
func (c *client) handleServerPreparedQuery(raw *frame.RawFrame, msg *message.Prepare, queryType string) {
	id := c.getQueryId(msg)

	if response, found := c.queryMetadataCache.get(id); found {
		c.sender.Send(raw.Header, response)
		return
	}

	var preparedQuery translator.IPreparedQuery
	var response *message.PreparedResult
	var err error
	switch queryType {
	case selectType:
		preparedQuery, response, err = c.prepareSelectType(raw, msg, id)
	case insertType:
		preparedQuery, response, err = c.prepareInsertType(raw, msg, id)
	case deleteType:
		preparedQuery, response, err = c.prepareDeleteType(raw, msg, id)
	case updateType:
		preparedQuery, response, err = c.prepareUpdateType(raw, msg, id)
	default:
		c.proxy.logger.Error("Unhandled Prepared CqlQuery Scenario", zap.String(Query, msg.Query))
		c.sender.Send(raw.Header, &message.Invalid{ErrorMessage: "Unhandled Prepared CqlQuery Scenario"})
		return
	}

	if err != nil {
		c.proxy.logger.Error(translatorErrorMessage, zap.String(Query, msg.Query), zap.Error(err))
		c.sender.Send(raw.Header, &message.Invalid{ErrorMessage: err.Error()})
		return
	}

	// update caches
	c.preparedQueries.set(id, preparedQuery)
	c.queryMetadataCache.set(id, response)

	c.sender.Send(raw.Header, response)
}

// function to handle and delete query of prepared type
func (c *client) prepareDeleteType(raw *frame.RawFrame, msg *message.Prepare, id [16]byte) (translator.IPreparedQuery, *message.PreparedResult, error) {
	deleteQueryMetadata, _, err := c.proxy.translator.TranslateDelete(msg.Query, true, c.keyspace)
	if err != nil {
		c.proxy.logger.Error(translatorErrorMessage, zap.String(Query, msg.Query), zap.Error(err))
		c.sender.Send(raw.Header, &message.Invalid{ErrorMessage: err.Error()})
		return nil, nil, err
	}
	c.AddQueryToCache(id, deleteQueryMetadata)
	return deleteQueryMetadata.ReturnMetadata, deleteQueryMetadata.VariableMetadata, err
}

// function to handle and insert query of prepared type
func (c *client) prepareInsertType(raw *frame.RawFrame, msg *message.Prepare, id [16]byte) (translator.IPreparedQuery, *message.PreparedResult, error) {
	insertQueryMetadata, _, err := c.proxy.translator.TranslateInsertQuery(msg.Query, true, c.keyspace)
	if err != nil {
		c.proxy.logger.Error(translatorErrorMessage, zap.String(Query, msg.Query), zap.Error(err))
		c.sender.Send(raw.Header, &message.Invalid{ErrorMessage: err.Error()})
		return nil, nil, err
	}
	return insertQueryMetadata.ReturnMetadata, insertQueryMetadata.VariableMetadata, err
}

// function to handle and select query of prepared type
func (c *client) prepareSelectType(raw *frame.RawFrame, msg *message.Prepare, id [16]byte) (translator.IPreparedQuery, *message.PreparedResult, error) {
	query, _, err := c.proxy.translator.PrepareSelect(msg.Query, c.keyspace, true, raw.Header.Version)
	query.CachedBTPrepare, err = c.proxy.bClient.PrepareStatement(c.ctx, query)
	if err != nil {
		return nil, nil, err
	}
	result, err := buildPreparedResultResponse(id, query.Keyspace(), query.Table(), query.Params, query.SelectedColumns)
	if err != nil {
		return nil, nil, err
	}
	return query, result, nil
}

// function to handle update query of prepared type
func (c *client) prepareUpdateType(raw *frame.RawFrame, msg *message.Prepare, id [16]byte) (translator.IPreparedQuery, *message.PreparedResult, error) {
	updateQueryMetadata, _, err := c.proxy.translator.PrepareUpdate(msg.Query, true, c.keyspace)
	if err != nil {
		c.proxy.logger.Error(translatorErrorMessage, zap.String(Query, msg.Query), zap.Error(err))
		c.sender.Send(raw.Header, &message.Invalid{ErrorMessage: err.Error()})
		return nil, err
	}
	md, err := buildPreparedResultResponse(id, updateQueryMetadata.Keyspace, updateQueryMetadata.Table, updateQueryMetadata.Params, nil)
	if err != nil {
		c.proxy.logger.Error(metadataFetchError, zap.String(Query, msg.Query), zap.Error(err))
		c.sender.Send(raw.Header, &message.ConfigError{ErrorMessage: err.Error()})
		return nil, err
	}
	// cache prepared query
	c.preparedQueries.set(id, updateQueryMetadata)
	return md, err
}

// handleExecute for prepared query
func (c *client) handleExecute(raw *frame.RawFrame, msg *partialExecute) {
	ctx := context.Background()
	id := preparedIdKey(msg.queryId)

	if stmt, ok := c.preparedSystemQuery.get(id); ok {
		c.interceptSystemQuery(raw.Header, stmt)
		return
	}

	preparedStmt, ok := c.preparedQueries.get(id)
	if !ok {
		c.proxy.logger.Error(errQueryNotPrepared)
		c.sender.Send(raw.Header, &message.ServerError{ErrorMessage: errQueryNotPrepared})
		return
	}

	var boundQuery translator.IBoundQuery
	var err error
	switch st := preparedStmt.(type) {
	case *translator.PreparedSelectQuery:
		boundQuery, err = c.proxy.translator.BindSelect(st, msg.PositionalValues, raw.Header.Version)
	case *translator.PreparedInsertQuery:
		boundQuery, err = c.proxy.translator.BindInsert(st, msg.PositionalValues, raw.Header.Version)
	case *translator.PreparedDeleteQuery:
		boundQuery, err = c.proxy.translator.BindDelete(st, msg.PositionalValues, raw.Header.Version)
	case *translator.PreparedUpdateQuery:
		boundQuery, err = c.proxy.translator.BindUpdate(st, msg.PositionalValues, raw.Header.Version)
	default:
		c.proxy.logger.Error("Unhandled Prepare Execute Scenario")
		c.sender.Send(raw.Header, &message.ServerError{ErrorMessage: "Unhandled Prepared CqlQuery Object"})
		return
	}

	if err != nil {
		c.proxy.logger.Error(errorWhileDecoding, zap.String(Query, preparedStmt.CqlQuery()), zap.Error(err))
		c.sender.Send(raw.Header, &message.ConfigError{ErrorMessage: err.Error()})
		return
	}

	results, err := c.proxy.bClient.Execute(ctx, boundQuery)
	if err != nil {
		c.proxy.logger.Error(errorAtBigtable, zap.String(Query, preparedStmt.CqlQuery()), zap.Error(err))
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
	defer c.proxy.otelInst.RecordMetrics(otelCtx, handleBatch, startTime, handleBatch, c.keyspace, otelErr)
	bulkMutations, keyspace, err := c.getAllPreparedOps(msg, raw.Header.Version)
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

func (c *client) getAllPreparedOps(msg *partialBatch, pv primitive.ProtocolVersion) (*bigtableModule.BigtableBulkMutation, types.Keyspace, error) {
	var keyspace types.Keyspace
	tableMutationsMap := bigtableModule.BigtableBulkMutation{}
	for index, queryId := range msg.queryOrIds {
		queryOrId, ok := queryId.([]byte)
		if !ok {
			return nil, "", fmt.Errorf("batch query id malformed")
		}
		id := preparedIdKey(queryOrId)
		if preparedStmt, ok := c.preparedQueries.get(id); ok {
			switch st := preparedStmt.(type) {
			case *translator.PreparedInsertQuery:
				keyspace = st.Keyspace
				bound, err := c.proxy.translator.BindInsert(st, msg.BatchPositionalValues[index], pv)
				if err != nil {
					return nil, "", err
				}
				tableMutationsMap.AddMutation(bound)
			case *translator.PreparedDeleteQuery:
				keyspace = st.Keyspace
				bound, err := c.proxy.translator.BindDelete(st, msg.BatchPositionalValues[index], pv)
				if err != nil {
					return nil, "", err
				}
				tableMutationsMap.AddMutation(bound)
			case *translator.PreparedUpdateQuery:
				keyspace = st.Keyspace
				bound, err := c.proxy.translator.BindUpdate(st, msg.BatchPositionalValues[index], pv)
				if err != nil {
					return nil, "", err
				}
				tableMutationsMap.AddMutation(bound)
			default:
				return nil, "", fmt.Errorf("unhandled prepared batch query object")
			}
		} else {
			return nil, "", fmt.Errorf("prepared query not found in cache")
		}
	}
	return &tableMutationsMap, keyspace, nil
}

func (c *client) handleQuery(raw *frame.RawFrame, msg *partialQuery) {
	startTime := time.Now()
	c.proxy.logger.Debug("handling query", zap.String("encodedQuery", msg.query), zap.Int16("stream", raw.Header.StreamId))

	handled, stmt, queryType, err := parser.IsQueryHandledWithQueryType(parser.IdentifierFromString(c.keyspace), msg.query)
	otelCtx, span := c.proxy.otelInst.StartSpan(c.proxy.ctx, handleQuery, []attribute.KeyValue{
		attribute.String("CqlQuery", msg.query),
	})
	defer c.proxy.otelInst.EndSpan(span)

	if handled {
		if err != nil {
			c.proxy.logger.Error("error parsing query to see if it's handled", zap.String(Query, msg.query), zap.Error(err))
			c.proxy.otelInst.RecordError(span, err)
			c.sender.Send(raw.Header, &message.Invalid{ErrorMessage: err.Error()})
			return
		}
		c.interceptSystemQuery(raw.Header, stmt)
		return
	} else {
		var otelErr error
		defer c.proxy.otelInst.RecordMetrics(otelCtx, handleQuery, startTime, queryType, c.keyspace, otelErr)

		switch queryType {
		case describeType:
			describeStmt, ok := stmt.(*parser.DescribeStatement)
			if !ok {
				c.sender.Send(raw.Header, &message.Invalid{ErrorMessage: "Invalid DESCRIBE statement"})
				return
			}
			if describeStmt.Keyspaces {
				c.handleDescribeKeyspaces(raw.Header)
			} else if describeStmt.Tables {
				c.handleDescribeTables(raw.Header)
			} else if describeStmt.TableName != "" {
				c.handleDescribeTable(raw.Header, describeStmt.KeyspaceName, describeStmt.TableName)
			} else if describeStmt.KeyspaceName != "" {
				c.handleDescribeKeyspace(raw.Header, describeStmt.KeyspaceName)
			} else {
				c.sender.Send(raw.Header, &message.Invalid{ErrorMessage: "Invalid DESCRIBE statement"})
			}
			return
		case selectType:
			_, bound, err := c.proxy.translator.PrepareSelect(msg.query, c.keyspace, false, raw.Header.Version)
			if err != nil {
				c.proxy.logger.Error(translatorErrorMessage, zap.String(Query, msg.query), zap.Error(err))
				otelErr = err
				c.proxy.otelInst.RecordError(span, otelErr)
				c.sender.Send(raw.Header, &message.Invalid{ErrorMessage: err.Error()})
				return
			}

			otelgo.AddAnnotation(otelCtx, executingBigtableSQLAPIRequestEvent)
			selectResult, err := c.proxy.bClient.SelectStatement(otelCtx, bound)
			otelgo.AddAnnotation(otelCtx, bigtableExecutionDoneEvent)

			if err != nil {
				c.proxy.logger.Error(errorAtBigtable, zap.String(Query, msg.query), zap.Error(err))
				otelErr = err
				c.proxy.otelInst.RecordError(span, otelErr)
				c.sender.Send(raw.Header, &message.Invalid{ErrorMessage: err.Error()})
				return
			}

			response, err := responsehandler.BuildResponse(selectResult)

			if err != nil {
				c.proxy.logger.Error(errorWhileDecoding, zap.String(Query, msg.query), zap.Error(err))
				otelErr = err
				c.proxy.otelInst.RecordError(span, otelErr)
				c.sender.Send(raw.Header, &message.Invalid{ErrorMessage: err.Error()})
				return
			}

			c.sender.Send(raw.Header, response)
			return

		case insertType:
			_, bound, err := c.proxy.translator.TranslateInsertQuery(msg.query, false, c.keyspace)
			if err != nil {
				c.proxy.logger.Error(translatorErrorMessage, zap.String(Query, msg.query), zap.Error(err))
				otelErr = err
				c.proxy.otelInst.RecordError(span, otelErr)
				c.sender.Send(raw.Header, &message.Invalid{ErrorMessage: err.Error()})
				return
			}

			otelgo.AddAnnotation(otelCtx, executingBigtableRequestEvent)
			resp, err := c.proxy.bClient.InsertRow(otelCtx, bound)
			otelgo.AddAnnotation(otelCtx, bigtableExecutionDoneEvent)

			if err != nil {
				c.proxy.logger.Error(errorAtBigtable, zap.String(Query, msg.query), zap.Error(err))
				otelErr = err
				c.proxy.otelInst.RecordError(span, otelErr)
				c.sender.Send(raw.Header, &message.Invalid{ErrorMessage: err.Error()})
				return
			}
			c.proxy.logger.Info("Data inserted successfully")
			if !bound.IfSpec.IfNotExists {
				c.sender.Send(raw.Header, &message.VoidResult{})
				return
			}
			c.sender.Send(raw.Header, resp)
			return

		case deleteType:
			_, bound, err := c.proxy.translator.TranslateDelete(msg.query, false, c.keyspace)
			if err != nil {
				c.proxy.logger.Error(translatorErrorMessage, zap.String(Query, msg.query), zap.Error(err))
				otelErr = err
				c.proxy.otelInst.RecordError(span, otelErr)
				c.sender.Send(raw.Header, &message.Invalid{ErrorMessage: err.Error()})
				return
			}

			otelgo.AddAnnotation(otelCtx, bigtableExecutionDoneEvent)
			result, dErr := c.proxy.bClient.DeleteRow(c.proxy.ctx, bound)
			if dErr != nil {
				c.proxy.logger.Error(errorAtBigtable, zap.String(Query, msg.query), zap.Error(dErr))
				otelErr = dErr
				c.proxy.otelInst.RecordError(span, otelErr)
				c.sender.Send(raw.Header, &message.Invalid{ErrorMessage: dErr.Error()})
				return
			} else {
				if !bound.IfExists {
					c.sender.Send(raw.Header, &message.VoidResult{})
					return
				}
				c.sender.Send(raw.Header, result)
				return
			}

		case updateType:
			_, bound, err := c.proxy.translator.PrepareUpdate(msg.query, false, c.keyspace)
			if err != nil {
				c.proxy.logger.Error(translatorErrorMessage, zap.String(Query, msg.query), zap.Error(err))
				otelErr = err
				c.proxy.otelInst.RecordError(span, otelErr)
				c.sender.Send(raw.Header, &message.Invalid{ErrorMessage: err.Error()})
				return
			}

			otelgo.AddAnnotation(otelCtx, executingBigtableRequestEvent)
			resp, err := c.proxy.bClient.UpdateRow(otelCtx, bound)
			otelgo.AddAnnotation(otelCtx, bigtableExecutionDoneEvent)

			if err != nil {
				c.proxy.logger.Error(errorAtBigtable, zap.String(Query, msg.query), zap.Error(err))
				otelErr = err
				c.proxy.otelInst.RecordError(span, otelErr)
				c.sender.Send(raw.Header, &message.Invalid{ErrorMessage: err.Error()})
				return
			}
			c.proxy.logger.Info("Data Updated successfully")
			if !bound.IfSpec.IfExists {
				c.sender.Send(raw.Header, &message.VoidResult{})
				return
			}
			c.sender.Send(raw.Header, resp)
			return

		case dropType:
			queryMetadata, err := c.proxy.translator.TranslateDropTableToBigtable(msg.query, c.keyspace)
			if err != nil {
				c.proxy.logger.Error(translatorErrorMessage, zap.String(Query, msg.query), zap.Error(err))
				otelErr = err
				c.proxy.otelInst.RecordError(span, otelErr)
				c.sender.Send(raw.Header, &message.Invalid{ErrorMessage: err.Error()})
				return
			}

			otelgo.AddAnnotation(otelCtx, executingBigtableRequestEvent)
			err = c.proxy.bClient.DropTable(c.proxy.ctx, queryMetadata)
			otelgo.AddAnnotation(otelCtx, bigtableExecutionDoneEvent)

			if err != nil {
				c.proxy.logger.Error(errorAtBigtable, zap.String(Query, msg.query), zap.Error(err))
				otelErr = err
				c.proxy.otelInst.RecordError(span, otelErr)
				c.sender.Send(raw.Header, &message.Invalid{ErrorMessage: err.Error()})
				return
			} else {
				c.handlePostDDLEvent(raw.Header, primitive.SchemaChangeTypeDropped, queryMetadata.Keyspace, queryMetadata.Table)
				c.sender.Send(raw.Header, &message.VoidResult{})
				return
			}

		case createType:
			queryMetadata, err := c.proxy.translator.TranslateCreateTableToBigtable(msg.query, c.keyspace)
			if err != nil {
				c.proxy.logger.Error(translatorErrorMessage, zap.String(Query, msg.query), zap.Error(err))
				otelErr = err
				c.proxy.otelInst.RecordError(span, otelErr)
				c.sender.Send(raw.Header, &message.Invalid{ErrorMessage: err.Error()})
				return
			}

			otelgo.AddAnnotation(otelCtx, executingBigtableRequestEvent)
			err = c.proxy.bClient.CreateTable(c.proxy.ctx, queryMetadata)
			otelgo.AddAnnotation(otelCtx, bigtableExecutionDoneEvent)

			if err != nil {
				c.proxy.logger.Error(errorAtBigtable, zap.String(Query, msg.query), zap.Error(err))
				otelErr = err
				c.proxy.otelInst.RecordError(span, otelErr)
				c.sender.Send(raw.Header, &message.Invalid{ErrorMessage: err.Error()})
				return
			} else {
				c.handlePostDDLEvent(raw.Header, primitive.SchemaChangeTypeCreated, queryMetadata.Keyspace, queryMetadata.Table)
				c.sender.Send(raw.Header, &message.VoidResult{})
				return
			}

		case truncateType:
			queryMetadata, err := c.proxy.translator.TranslateTruncateTableToBigtable(msg.query, c.keyspace)
			if err != nil {
				c.proxy.logger.Error(translatorErrorMessage, zap.String(Query, msg.query), zap.Error(err))
				otelErr = err
				c.proxy.otelInst.RecordError(span, otelErr)
				c.sender.Send(raw.Header, &message.Invalid{ErrorMessage: err.Error()})
				return
			}

			otelgo.AddAnnotation(otelCtx, bigtableExecutionDoneEvent)
			err = c.proxy.bClient.DropAllRows(c.proxy.ctx, queryMetadata)
			if err != nil {
				c.proxy.logger.Error(errorAtBigtable, zap.String(Query, msg.query), zap.Error(err))
				otelErr = err
				c.proxy.otelInst.RecordError(span, otelErr)
				c.sender.Send(raw.Header, &message.Invalid{ErrorMessage: err.Error()})
				return
			} else {
				c.sender.Send(raw.Header, &message.VoidResult{})
				return
			}

		case alterType:
			queryMetadata, err := c.proxy.translator.TranslateAlterTableToBigtable(msg.query, c.keyspace)

			if err != nil {
				c.proxy.logger.Error(translatorErrorMessage, zap.String(Query, msg.query), zap.Error(err))
				otelErr = err
				c.proxy.otelInst.RecordError(span, otelErr)
				c.sender.Send(raw.Header, &message.Invalid{ErrorMessage: err.Error()})
				return
			}

			otelgo.AddAnnotation(otelCtx, executingBigtableRequestEvent)
			err = c.proxy.bClient.AlterTable(c.proxy.ctx, queryMetadata)
			otelgo.AddAnnotation(otelCtx, bigtableExecutionDoneEvent)

			if err != nil {
				c.proxy.logger.Error(errorAtBigtable, zap.String(Query, msg.query), zap.Error(err))
				otelErr = err
				c.proxy.otelInst.RecordError(span, otelErr)
				c.sender.Send(raw.Header, &message.Invalid{ErrorMessage: err.Error()})
				return
			} else {
				c.handlePostDDLEvent(raw.Header, primitive.SchemaChangeTypeUpdated, queryMetadata.Keyspace, queryMetadata.Table)
				c.sender.Send(raw.Header, &message.VoidResult{})
				return
			}

		default:
			otelErr = fmt.Errorf("invalid query type: %s", queryType)
			c.proxy.otelInst.RecordError(span, otelErr)
			c.proxy.logger.Error(otelErr.Error(), zap.String(Query, msg.query))
			c.sender.Send(raw.Header, &message.Invalid{ErrorMessage: otelErr.Error()})
			return
		}
	}
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
// - s: *parser.SelectStatement (keyspace and table info)
//
// Returns:
// - []message.Row: Metadata rows for the requested table; empty if keyspace/table is invalid.
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

// Intercept and handle system query
func (c *client) interceptSystemQuery(hdr *frame.Header, stmt interface{}) {
	switch s := stmt.(type) {
	case *parser.SelectStatement:
		if s.Keyspace == systemSchema || s.Keyspace == systemVirtualSchema {
			var localColumns []*message.ColumnMetadata
			var isFound bool
			if s.Keyspace == systemSchema {
				localColumns, isFound = parser.SystemSchematablesColumn[s.Table]
				if isFound {
					tableMetadata := &message.RowsMetadata{
						ColumnCount: int32(len(localColumns)),
						Columns:     localColumns,
					}

					data, err := c.getSystemMetadata(hdr, s)
					if err != nil {
						c.sender.Send(hdr, &message.Invalid{ErrorMessage: err.Error()})
						return
					}

					c.sender.Send(hdr, &message.RowsResult{
						Metadata: tableMetadata,
						Data:     data,
					})
					return
				}
			} else {
				// get Table metadata for system_virtual_schema schema
				localColumns, isFound = parser.SystemVirtualSchemaColumn[s.Table]
				if isFound {
					c.sender.Send(hdr, &message.RowsResult{
						Metadata: &message.RowsMetadata{
							ColumnCount: int32(len(localColumns)),
							Columns:     localColumns,
						},
					})
					return
				}
			}
			if !isFound {
				c.sender.Send(hdr, &message.Invalid{ErrorMessage: "Error while fetching mocked table info"})
				return
			}
		} else if s.Table == local {
			localColumns := parser.SystemLocalColumns
			if len(c.proxy.cluster.Info.DSEVersion) > 0 {
				localColumns = parser.DseSystemLocalColumns
			}
			if columns, err := parser.FilterColumns(s, localColumns); err != nil {
				c.sender.Send(hdr, &message.Invalid{ErrorMessage: err.Error()})
			} else if row, err := c.filterSystemLocalValues(s, columns); err != nil {
				c.sender.Send(hdr, &message.Invalid{ErrorMessage: err.Error()})
			} else {
				c.sender.Send(hdr, &message.RowsResult{
					Metadata: &message.RowsMetadata{
						ColumnCount: int32(len(columns)),
						Columns:     columns,
					},
					Data: []message.Row{row},
				})
			}
		} else if s.Table == "peers" {
			peersColumns := parser.SystemPeersColumns
			if len(c.proxy.cluster.Info.DSEVersion) > 0 {
				peersColumns = parser.DseSystemPeersColumns
			}
			if columns, err := parser.FilterColumns(s, peersColumns); err != nil {
				c.sender.Send(hdr, &message.Invalid{ErrorMessage: err.Error()})
			} else {
				var data []message.Row
				for _, n := range c.proxy.nodes {
					if n != c.proxy.localNode {
						var row message.Row
						row, err = c.filterSystemPeerValues(s, columns, n, len(c.proxy.nodes)-1)
						if err != nil {
							break
						}
						data = append(data, row)
					}
				}
				if err != nil {
					c.sender.Send(hdr, &message.Invalid{ErrorMessage: err.Error()})
				} else {
					c.sender.Send(hdr, &message.RowsResult{
						Metadata: &message.RowsMetadata{
							ColumnCount: int32(len(columns)),
							Columns:     columns,
						},
						Data: data,
					})
				}
			}
			// CC- metadata is mocked here as well for system queries
		} else if columns, ok := parser.SystemColumnsByName[s.Table]; ok {
			c.sender.Send(hdr, &message.RowsResult{
				Metadata: &message.RowsMetadata{
					ColumnCount: int32(len(columns)),
					Columns:     columns,
				},
			})
		} else {
			c.sender.Send(hdr, &message.Invalid{ErrorMessage: "Doesn't exist"})
		}
	case *parser.UseStatement:
		c.keyspace = strings.Trim(s.Keyspace, "\" ")
		c.sender.Send(hdr, &message.SetKeyspaceResult{Keyspace: s.Keyspace})
	default:
		c.sender.Send(hdr, &message.ServerError{ErrorMessage: "Proxy attempted to intercept an unhandled query"})
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
func NewDefaultPreparedCache(size int) (proxycore.PreparedCache, error) {
	cache, err := lru.New(size)
	if err != nil {
		return nil, err
	}
	return &defaultPreparedCache{cache}, nil
}

type defaultPreparedCache struct {
	cache *lru.Cache
}

func (d defaultPreparedCache) Store(id string, entry *proxycore.PreparedEntry) {
	d.cache.Add(id, entry)
}

func (d defaultPreparedCache) Load(id string) (entry *proxycore.PreparedEntry, ok bool) {
	if val, ok := d.cache.Get(id); ok {
		return val.(*proxycore.PreparedEntry), true
	}
	return nil, false
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

// handleDescribeKeyspaces handles the DESCRIBE KEYSPACES command
func (c *client) handleDescribeKeyspaces(hdr *frame.Header) {
	// Get all keyspaces from the schema mapping
	keyspaces := make([]string, 0, len(c.proxy.schemaMapping.GetAllTables()))

	// Add custom keyspaces from schema mapping
	for keyspace := range c.proxy.schemaMapping.GetAllTables() {
		keyspaces = append(keyspaces, keyspace)
	}

	// Sort the keyspaces for consistent output
	sort.Strings(keyspaces)

	columns := []*message.ColumnMetadata{
		{
			Name:     "name", // Changed from "keyspace_name" to "name" to match cqlsh's expectation
			Type:     datatype.Varchar,
			Table:    "keyspaces",
			Keyspace: "system_virtual_schema",
		},
	}
	var rows []message.Row
	for _, ks := range keyspaces {
		rows = append(rows, message.Row{[]byte(ks)})
	}
	c.sender.Send(hdr, &message.RowsResult{
		Metadata: &message.RowsMetadata{
			ColumnCount: int32(len(columns)),
			Columns:     columns,
		},
		Data: rows,
	})
}

// handleDescribeTables handles the DESCRIBE TABLES command
func (c *client) handleDescribeTables(hdr *frame.Header) {
	// Return a list of tables in
	tables := []struct {
		keyspace string
		table    string
	}{
		{"system_virtual_schema", "keyspaces"},
		{"system_virtual_schema", "tables"},
		{"system_virtual_schema", "metaDataColumns"},
	}

	for _, keyspaceTables := range c.proxy.schemaMapping.GetAllTables() {
		for _, table := range keyspaceTables {
			tables = append(tables, struct {
				keyspace string
				table    string
			}{table.Keyspace, table.Name})
		}
	}

	columns := []*message.ColumnMetadata{
		{
			Name:     "keyspace_name",
			Type:     datatype.Varchar,
			Table:    "tables",
			Keyspace: "system",
		},
		{
			Name:     "name",
			Type:     datatype.Varchar,
			Table:    "tables",
			Keyspace: "system",
		},
	}
	var rows []message.Row
	for _, t := range tables {
		rows = append(rows, message.Row{
			[]byte(t.keyspace),
			[]byte(t.table),
		})
	}
	c.sender.Send(hdr, &message.RowsResult{
		Metadata: &message.RowsMetadata{
			ColumnCount: int32(len(columns)),
			Columns:     columns,
		},
		Data: rows,
	})
}

// handleDescribeTable handles the DESCRIBE TABLE command for a specific table
func (c *client) handleDescribeTable(hdr *frame.Header, keyspace, table string) {
	tableConfig, err := c.proxy.schemaMapping.GetTableConfig(keyspace, table)
	if err != nil {
		c.sender.Send(hdr, &message.Invalid{ErrorMessage: fmt.Sprintf("Error getting column metadata: %v", err)})
		return
	}

	columns := []*message.ColumnMetadata{
		{
			Name:     "create_statement",
			Type:     datatype.Varchar,
			Table:    tableConfig.Name,
			Keyspace: tableConfig.Keyspace,
		},
	}

	stmt := tableConfig.Describe()

	c.sender.Send(hdr, &message.RowsResult{
		Metadata: &message.RowsMetadata{
			ColumnCount: int32(len(columns)),
			Columns:     columns,
		},
		Data: []message.Row{{[]byte(stmt)}},
	})
}

// handleDescribeKeyspace handles the DESCRIBE KEYSPACE <keyspace> command
func (c *client) handleDescribeKeyspace(hdr *frame.Header, keyspaceName string) {
	columns := []*message.ColumnMetadata{
		{
			Name:     "create_statement",
			Type:     datatype.Varchar,
			Table:    "keyspaces",
			Keyspace: keyspaceName,
		},
	}

	createStmts := []string{fmt.Sprintf("CREATE KEYSPACE %s WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor' : 1};", keyspaceName)}

	tables, err := c.proxy.schemaMapping.GetKeyspace(keyspaceName)
	if err != nil {
		c.sender.Send(hdr, &message.Invalid{ErrorMessage: err.Error()})
		return
	}

	for _, tableConfig := range tables {
		createTableStmt := tableConfig.Describe()
		createStmts = append(createStmts, createTableStmt)
	}

	var rows []message.Row
	for _, stmt := range createStmts {
		rows = append(rows, message.Row{[]byte(stmt)})
	}

	c.sender.Send(hdr, &message.RowsResult{
		Metadata: &message.RowsMetadata{
			ColumnCount: int32(len(columns)),
			Columns:     columns,
		},
		Data: rows,
	})
}

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

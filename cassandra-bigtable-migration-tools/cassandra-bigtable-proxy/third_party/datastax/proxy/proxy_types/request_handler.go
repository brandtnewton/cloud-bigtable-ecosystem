package proxy_types

import (
	"encoding/hex"
	bigtableModule "github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/bigtable"
	"github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/executors"
	"github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/global/types"
	otelgo "github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/otel"
	"github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/third_party/datastax/proxycore"
	"github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/translators"
	"github.com/datastax/go-cassandra-native-protocol/message"
	"github.com/datastax/go-cassandra-native-protocol/primitive"
	"go.uber.org/zap"
	"sync"
)

// Constants moved from proxy package
const (
	Query               = "CqlQuery"
	QueryTypeConst      = "QueryType" // Renamed from QueryType to avoid collision with types.QueryType
	ErrQueryNotPrepared = "query is not prepared"
)

// Trace names
const (
	TraceNamespace = "cassandra.bigtable.proxy"
	HandleQuery    = TraceNamespace + "/HandleQuery"
	HandleBatch    = TraceNamespace + "/ExecuteBatch"
	HandlePrepare  = TraceNamespace + "/PrepareQuery"
	HandleExecute  = TraceNamespace + "/ExecuteQuery"
	HandleRegister = TraceNamespace + "/Register"
	HandleStartup  = TraceNamespace + "/Startup"
	HandleOptions  = TraceNamespace + "/Options"
)

// Events
const (
	ExecutingBigtableRequestEvent       = "Executing Bigtable Mutation Request"
	ExecutingBigtableSQLAPIRequestEvent = "Executing Bigtable SQL API Request"
	BigtableExecutionDoneEvent          = "bigtable Execution Done"
	GotBulkApplyResp                    = "Got the response for bulk apply"
	SendingBulkApplyMutation            = "Sending Mutation For Bulk Apply"
)

type IProxy interface {
	Config() *types.ProxyInstanceConfig
	OtelInst() *otelgo.OpenTelemetry
	Logger() *zap.Logger
	Translator() *translators.TranslatorManager
	Executor() *executors.QueryExecutorManager
	PreparedQueryCache() proxycore.PreparedCache[types.IPreparedQuery]
	BigtableClient() *bigtableModule.BigtableAdapter
	EventClients() *sync.Map
}

func PreparedIdKey(bytes []byte) [16]byte {
	var buf [16]byte
	copy(buf[:], bytes)
	return buf
}

// Partial types moved from codecs.go

type PartialQuery struct {
	Query string
}

func (p *PartialQuery) IsResponse() bool {
	return false
}

func (p *PartialQuery) GetOpCode() primitive.OpCode {
	return primitive.OpCodeQuery
}

func (p *PartialQuery) Clone() message.Message {
	return &PartialQuery{p.Query}
}

type PartialExecute struct {
	QueryId          []byte
	PositionalValues []*primitive.Value
	NamedValues      map[string]*primitive.Value
}

func (m *PartialExecute) IsResponse() bool {
	return false
}

func (m *PartialExecute) GetOpCode() primitive.OpCode {
	return primitive.OpCodeExecute
}

func (m *PartialExecute) Clone() message.Message {
	return &PartialExecute{
		QueryId: primitive.CloneByteSlice(m.QueryId),
	}
}

func (m *PartialExecute) String() string {
	return "EXECUTE " + hex.EncodeToString(m.QueryId)
}

type PartialBatch struct {
	QueryOrIds            []interface{}
	BatchPositionalValues [][]*primitive.Value
}

func (p *PartialBatch) IsResponse() bool {
	return false
}

func (p *PartialBatch) GetOpCode() primitive.OpCode {
	return primitive.OpCodeBatch
}

func (p *PartialBatch) Clone() message.Message {
	queryOrIds := make([]interface{}, len(p.QueryOrIds))
	copy(queryOrIds, p.QueryOrIds)
	positionalValues := make([][]*primitive.Value, len(p.BatchPositionalValues))
	copy(positionalValues, p.BatchPositionalValues)
	return &PartialBatch{queryOrIds, positionalValues}
}

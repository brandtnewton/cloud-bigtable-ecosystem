package proxy

import (
	"context"
	"github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/global/types"
	otelgo "github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/otel"
	"github.com/datastax/go-cassandra-native-protocol/frame"
	"github.com/datastax/go-cassandra-native-protocol/message"
	"github.com/datastax/go-cassandra-native-protocol/primitive"
	"go.opentelemetry.io/otel/trace"
	"time"
)

type OptionsRequestHandler struct {
}

func (o *OptionsRequestHandler) Name() string {
	return "options"
}

func (o *OptionsRequestHandler) OpCode() primitive.OpCode {
	return primitive.OpCodeOptions
}

func (o *OptionsRequestHandler) HandleRequest(ctx context.Context, c *client, raw *frame.RawFrame, m message.Message) (message.Message, error) {
	span := trace.SpanFromContext(ctx)
	startTime := time.Now()
	var otelErr error
	defer func() {
		attrs := types.Attributes{
			Method:   handleOptions,
			Keyspace: c.sessionKeyspace,
			Status:   otelgoStatus(otelErr),
		}
		otelgo.AddQueryAnnotations(span, attrs)
		c.proxy.otelInst.RecordMetrics(ctx, startTime, attrs)
	}()

	return &message.Supported{Options: map[string][]string{
		"CQL_VERSION": {c.proxy.config.Options.CQLVersion},
		"COMPRESSION": {},
	}}, nil
}

func NewOptionsRequestHandler() IProxyRequestHandler {
	return &OptionsRequestHandler{}
}

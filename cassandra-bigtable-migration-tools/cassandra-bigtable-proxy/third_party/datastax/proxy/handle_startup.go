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

type StartupRequestHandler struct {
}

func (s *StartupRequestHandler) Name() string {
	return "startup"
}

func (s *StartupRequestHandler) OpCode() primitive.OpCode {
	return primitive.OpCodeStartup
}

func (s *StartupRequestHandler) HandleRequest(ctx context.Context, c *client, raw *frame.RawFrame, m message.Message) (message.Message, error) {
	span := trace.SpanFromContext(ctx)
	startTime := time.Now()
	var otelErr error
	defer func() {
		attrs := types.Attributes{
			Method:   handleStartup,
			Keyspace: c.sessionKeyspace,
			Status:   otelgoStatus(otelErr),
		}
		otelgo.AddQueryAnnotations(span, attrs)
		c.proxy.otelInst.RecordMetrics(ctx, startTime, attrs)
	}()

	// CC -  register for Event types and respond READY
	return &message.Ready{}, nil
}

func NewStartupRequestHandler() IProxyRequestHandler {
	return &StartupRequestHandler{}
}

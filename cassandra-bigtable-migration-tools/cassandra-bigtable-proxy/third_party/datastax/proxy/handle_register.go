package proxy

import (
	"context"
	"github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/global/types"
	otelgo "github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/otel"
	"github.com/datastax/go-cassandra-native-protocol/frame"
	"github.com/datastax/go-cassandra-native-protocol/message"
	"github.com/datastax/go-cassandra-native-protocol/primitive"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
	"time"
)

type RegisterRequestHandler struct {
}

func (r *RegisterRequestHandler) Name() string {
	return "register"
}

func (r *RegisterRequestHandler) OpCode() primitive.OpCode {
	return primitive.OpCodeRegister
}

func (r *RegisterRequestHandler) HandleRequest(ctx context.Context, c *client, raw *frame.RawFrame, m message.Message) (message.Message, error) {
	msg := m.(*message.Register)
	span := trace.SpanFromContext(ctx)
	startTime := time.Now()
	var otelErr error
	defer func() {
		attrs := types.Attributes{
			Method:   handleRegister,
			Keyspace: c.sessionKeyspace,
			Status:   otelgoStatus(otelErr),
		}
		otelgo.AddQueryAnnotations(span, attrs)
		c.proxy.otelInst.RecordMetrics(ctx, startTime, attrs)
	}()

	c.proxy.logger.Info("Client registered for events", zap.Any("event_types", msg.EventTypes))
	for _, t := range msg.EventTypes {
		if t == primitive.EventTypeSchemaChange {
			c.proxy.registerForEvents(c)
		}
	}
	return &message.Ready{}, nil
}

func NewRegisterRequestHandler() IProxyRequestHandler {
	return &RegisterRequestHandler{}
}

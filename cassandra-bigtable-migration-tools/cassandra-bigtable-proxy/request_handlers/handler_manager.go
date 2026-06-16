package request_handlers

import (
	"context"
	"fmt"
	"github.com/datastax/go-cassandra-native-protocol/frame"
	"github.com/datastax/go-cassandra-native-protocol/message"
	"github.com/datastax/go-cassandra-native-protocol/primitive"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

type HandlerManager struct {
	requestHandlers map[primitive.OpCode]IProxyRequestHandler
}

func NewHandlerManager() *HandlerManager {
	return &HandlerManager{
		requestHandlers: make(map[primitive.OpCode]IProxyRequestHandler),
	}
}

func (h *HandlerManager) InitHandlers(server IProxyServer) {
	handlers := []IProxyRequestHandler{
		NewOptionsRequestHandler(server),
		NewStartupRequestHandler(),
		NewRegisterRequestHandler(),
		NewPrepareRequestHandler(server),
		NewExecuteRequestHandler(server),
		NewQueryRequestHandler(server),
		NewBatchRequestHandler(server),
	}
	handlersLookup := make(map[primitive.OpCode]IProxyRequestHandler)
	for _, handler := range handlers {
		handlersLookup[handler.OpCode()] = handler
	}
}

func (h *HandlerManager) HandleRequest(ctx context.Context, session IProxySession, raw *frame.RawFrame, m message.Message) message.Message {
	span := trace.SpanFromContext(ctx)
	handler, ok := h.requestHandlers[raw.Header.OpCode]
	if !ok {
		return &message.ServerError{ErrorMessage: fmt.Sprintf("unsupported operation: %s", raw.Header.OpCode.String())}
	}
	result, err := handler.HandleRequest(ctx, session, raw, m)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return &message.ServerError{ErrorMessage: err.Error()}
	}
	return result
}

package request_handlers

import (
	"context"
	"fmt"
	"github.com/datastax/go-cassandra-native-protocol/message"
	"github.com/datastax/go-cassandra-native-protocol/primitive"
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
	h.requestHandlers = make(map[primitive.OpCode]IProxyRequestHandler)
	for _, handler := range handlers {
		h.requestHandlers[handler.OpCode()] = handler
	}
}

func (h *HandlerManager) HandleRequest(ctx context.Context, session IProxySession, req *ProxyRequest) (message.Message, error) {
	req.Attributes.Method = opCodeToNiceString(req.header.OpCode)
	handler, ok := h.requestHandlers[req.header.OpCode]
	if !ok {
		return nil, fmt.Errorf("unsupported operation: %s", opCodeToNiceString(req.header.OpCode))
	}
	return handler.HandleRequest(ctx, session, req)
}

func opCodeToNiceString(c primitive.OpCode) string {
	switch c {
	case primitive.OpCodeStartup:
		return "STARTUP"
	case primitive.OpCodeOptions:
		return "OPTIONS"
	case primitive.OpCodeQuery:
		return "QUERY"
	case primitive.OpCodePrepare:
		return "PREPARE"
	case primitive.OpCodeExecute:
		return "EXECUTE"
	case primitive.OpCodeRegister:
		return "REGISTER"
	case primitive.OpCodeBatch:
		return "BATCH"
	case primitive.OpCodeAuthResponse:
		return "AUTH RESPONSE"
	case primitive.OpCodeDseRevise:
		return "REVISE"
		// responses
	case primitive.OpCodeError:
		return "ERROR"
	case primitive.OpCodeReady:
		return "READY"
	case primitive.OpCodeAuthenticate:
		return "AUTHENTICATE"
	case primitive.OpCodeSupported:
		return "SUPPORTED"
	case primitive.OpCodeResult:
		return "RESULT"
	case primitive.OpCodeEvent:
		return "EVENT"
	case primitive.OpCodeAuthChallenge:
		return "AUTH CHALLENGE"
	case primitive.OpCodeAuthSuccess:
		return "AUTH SUCCESS"
	}
	return "UNKNOWN"
}

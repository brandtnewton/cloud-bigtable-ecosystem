package request_handlers

import (
	"context"
	"github.com/datastax/go-cassandra-native-protocol/message"
	"github.com/datastax/go-cassandra-native-protocol/primitive"
)

type StartupRequestHandler struct {
}

func (s *StartupRequestHandler) Name() string {
	return "startup"
}

func (s *StartupRequestHandler) OpCode() primitive.OpCode {
	return primitive.OpCodeStartup
}

func (s *StartupRequestHandler) HandleRequest(_ context.Context, _ IProxySession, req *ProxyRequest) (message.Message, error) {
	// CC -  register for Event types and respond READY
	return &message.Ready{}, nil
}

func NewStartupRequestHandler() IProxyRequestHandler {
	return &StartupRequestHandler{}
}

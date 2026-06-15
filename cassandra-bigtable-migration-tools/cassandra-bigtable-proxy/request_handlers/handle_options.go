package request_handlers

import (
	"context"
	"github.com/datastax/go-cassandra-native-protocol/frame"
	"github.com/datastax/go-cassandra-native-protocol/message"
	"github.com/datastax/go-cassandra-native-protocol/primitive"
)

type OptionsRequestHandler struct {
	server IProxyServer
}

func (o *OptionsRequestHandler) Name() string {
	return "options"
}

func (o *OptionsRequestHandler) OpCode() primitive.OpCode {
	return primitive.OpCodeOptions
}

func (o *OptionsRequestHandler) HandleRequest(ctx context.Context, session IProxySession, raw *frame.RawFrame, m message.Message) (message.Message, error) {
	return &message.Supported{Options: map[string][]string{
		"CQL_VERSION": {o.server.CQLVersion()},
		"COMPRESSION": {},
	}}, nil
}

func NewOptionsRequestHandler(server IProxyServer) IProxyRequestHandler {
	return &OptionsRequestHandler{server: server}
}

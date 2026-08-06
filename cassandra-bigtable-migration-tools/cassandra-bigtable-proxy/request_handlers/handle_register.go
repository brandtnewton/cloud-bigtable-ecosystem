package request_handlers

import (
	"context"
	"github.com/datastax/go-cassandra-native-protocol/message"
	"github.com/datastax/go-cassandra-native-protocol/primitive"
)

type RegisterRequestHandler struct {
}

func (r *RegisterRequestHandler) Name() string {
	return "register"
}

func (r *RegisterRequestHandler) OpCode() primitive.OpCode {
	return primitive.OpCodeRegister
}

func (r *RegisterRequestHandler) HandleRequest(_ context.Context, session IProxySession, req *ProxyRequest) (message.Message, error) {
	msg := req.msg.(*message.Register)
	for _, t := range msg.EventTypes {
		if t == primitive.EventTypeSchemaChange {
			session.RegisterForEvents()
		}
	}
	return &message.Ready{}, nil
}

func NewRegisterRequestHandler() IProxyRequestHandler {
	return &RegisterRequestHandler{}
}

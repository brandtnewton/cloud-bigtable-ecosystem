package proxy
import (
	"context"
	"github.com/datastax/go-cassandra-native-protocol/frame"
	"github.com/datastax/go-cassandra-native-protocol/message"
	"github.com/datastax/go-cassandra-native-protocol/primitive"
)

type IProxyRequestHandler interface {
	Name() string
	OpCode() primitive.OpCode
	HandleRequest(ctx context.Context, c *client, raw *frame.RawFrame, m message.Message) (message.Message, error)
}

func otelgoStatus(err error) string {
	if err != nil {
		return "ERROR"
	}
	return "OK"
}

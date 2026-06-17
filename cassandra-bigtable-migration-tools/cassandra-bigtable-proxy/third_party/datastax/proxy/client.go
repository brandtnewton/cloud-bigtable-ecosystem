// Copyright (c) DataStax, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package proxy

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/global/types"
	otelgo "github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/otel"
	"github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/request_handlers"
	"github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/third_party/datastax/proxy/proxy_types"
	"github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/third_party/datastax/proxycore"
	"github.com/datastax/go-cassandra-native-protocol/frame"
	"github.com/datastax/go-cassandra-native-protocol/message"
	"github.com/datastax/go-cassandra-native-protocol/primitive"
	"go.opentelemetry.io/otel/codes"
	"go.uber.org/zap"
	"io"
	"runtime/debug"
)

type Sender interface {
	Send(hdr *frame.Header, msg message.Message)
}

type client struct {
	sessionKeyspace types.Keyspace
	proxy           *Server
	conn            *proxycore.Conn
	sender          Sender
}

func (c *client) SetSessionKeyspace(k types.Keyspace) {
	c.sessionKeyspace = k
}

func (c *client) SessionKeyspace() types.Keyspace {
	return c.sessionKeyspace
}

func (c *client) Proxy() proxy_types.IProxy {
	return c.proxy
}

func (c *client) RegisterForEvents() {
	c.proxy.registerForEvents(c)
}

func (c *client) Receive(reader io.Reader) error {
	ctx, cancel := context.WithCancel(c.proxy.ctx)
	defer cancel()

	otelCtx, span := c.proxy.tracer.Start(ctx, "receive")
	defer span.End()
	span.AddEvent("decode-raw-request")
	raw, err := codec.DecodeRawFrame(reader)
	if err != nil {
		if !errors.Is(err, io.EOF) {
			c.proxy.logger.Error("unable to decode frame", zap.Error(err))
		}
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return err
	}

	defer func() {
		if r := recover(); r != nil {
			fmt.Printf("Trapped a panic: %v\n", r)

			fmt.Println("Stack Trace:")
			debug.PrintStack()

			c.sender.Send(raw.Header, &message.ProtocolError{
				ErrorMessage: "internal error",
			})

			span.SetStatus(codes.Error, "internal error")
		}
	}()

	if raw.Header.Version > c.proxy.config.Options.MaxProtocolVersion || raw.Header.Version < primitive.ProtocolVersion3 {
		err = fmt.Errorf("invalid or unsupported protocol version %d", raw.Header.Version)
		c.sender.Send(raw.Header, &message.ProtocolError{
			ErrorMessage: err.Error(),
		})
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return nil
	}

	span.AddEvent("decode-request-body")
	body, err := codec.DecodeBody(raw.Header, bytes.NewReader(raw.Body))
	if err != nil {
		c.proxy.logger.Error("unable to decode body", zap.Error(err))
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return err
	}

	span.AddEvent("handle-request")
	req := request_handlers.NewProxyRequest(raw.Header, body.Message)
	response, err := c.proxy.handlerManager.HandleRequest(otelCtx, c, req)
	if err != nil {
		span.SetAttributes(otelgo.CommonAttributes(req.Attributes)...)
		c.proxy.logger.Error("error handling request", zap.Error(err))
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return err
	}

	span.SetAttributes(otelgo.CommonAttributes(req.Attributes)...)
	span.AddEvent("send-response")
	c.sender.Send(raw.Header, response)
	span.AddEvent("done")
	span.SetStatus(codes.Ok, "")
	return nil
}

func (c *client) Send(hdr *frame.Header, msg message.Message) {
	_ = c.conn.Write(proxycore.SenderFunc(func(writer io.Writer) error {
		return codec.EncodeFrame(frame.NewFrame(hdr.Version, hdr.StreamId, msg), writer)
	}))
}

func (c *client) Closing(_ error) {
	c.proxy.removeClient(c)
}

// handleEvent handles events from the proxy core
// It sends the event message to all connected clients.
func (c *client) handleEvent(event proxycore.Event) {
	switch evt := event.(type) {
	case *proxycore.SchemaChangeEvent:
		c.sender.Send(&frame.Header{
			Version:  c.proxy.config.Options.ProtocolVersion,
			StreamId: -1, // -1 for events
			OpCode:   primitive.OpCodeEvent,
		}, evt.Message)
	}
}

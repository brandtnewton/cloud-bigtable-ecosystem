/*
 * Copyright (C) 2025 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package otelgo

import (
	"context"
	"fmt"
	"github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/global/types"
	"go.opentelemetry.io/otel/trace"
	"net/http"
	"net/url"
	"strings"

	"github.com/google/uuid"
	"go.opentelemetry.io/contrib/detectors/gcp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/resource"
	semconv "go.opentelemetry.io/otel/semconv/v1.4.0"
	"go.uber.org/zap"
)

type ShutdownFn func(ctx context.Context) error

var (
	attributeKeyMethod    = attribute.Key("method")
	attributeKeyStatus    = attribute.Key("status")
	attributeKeyKeyspace  = attribute.Key("keyspace")
	attributeKeyQueryType = attribute.Key("query_type")
	attributeKeyTable     = attribute.Key("table")
)

type OpenTelemetry struct {
	Config *types.OtelConfig
	logger *zap.Logger
}

// NewOpenTelemetry initializes OpenTelemetry tracing components.
func NewOpenTelemetry(ctx context.Context, config *types.OtelConfig, logger *zap.Logger) (*OpenTelemetry, ShutdownFn, error) {
	otelInst := &OpenTelemetry{Config: config, logger: logger}

	if config.Enabled && config.HealthCheck.Enabled {
		resp, err := http.Get("http://" + config.HealthCheck.Endpoint)
		if err != nil || resp.StatusCode != 200 {
			return nil, nil, fmt.Errorf("OTEL health check failed: %v", err)
		}
		logger.Info("OTEL health check complete")
	}

	res := buildOtelResource(ctx, config)

	tp, shutdownTp, err := createTraceProvider(ctx, config, res)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create trace provider: %w", err)
	}
	otel.SetTracerProvider(tp)

	return otelInst, shutdownTp, nil
}

func buildOtelResource(ctx context.Context, config *types.OtelConfig) *resource.Resource {
	attrs := []attribute.KeyValue{
		semconv.ServiceNameKey.String(config.ServiceName),
		semconv.ServiceInstanceIDKey.String(uuid.New().String()),
		semconv.ServiceVersionKey.String(config.ServiceVersion),
	}
	res, err := resource.New(ctx,
		resource.WithSchemaURL(semconv.SchemaURL),
		resource.WithDetectors(gcp.NewDetector()),
		resource.WithTelemetrySDK(),
		resource.WithAttributes(attrs...),
	)
	if err != nil {
		return resource.NewWithAttributes(semconv.SchemaURL, attrs...)
	}
	return res
}

func isValidEndpoint(endpoint string) bool {
	if endpoint == "" {
		return false
	}
	if strings.Contains(endpoint, "://") {
		u, err := url.Parse(endpoint)
		return err == nil && u.Host != "" && u.Port() != ""
	}
	parts := strings.Split(endpoint, ":")
	return len(parts) == 2 && parts[0] != "" && parts[1] != ""
}

func CommonAttributes(attrs types.Attributes) []attribute.KeyValue {
	return []attribute.KeyValue{
		attributeKeyKeyspace.String(string(attrs.Keyspace)),
		attributeKeyMethod.String(attrs.Method),
		attributeKeyQueryType.String(attrs.QueryType.String()),
		attributeKeyTable.String(string(attrs.Table)),
		attributeKeyStatus.String(attrs.Status),
	}
}

func AddQueryAnnotations(s trace.Span, q types.IQuery) {
	s.SetAttributes(
		attributeKeyQueryType.String(q.QueryType().String()),
		attributeKeyKeyspace.String(string(q.Keyspace())),
		attributeKeyTable.String(string(q.Table())),
	)
}

func AddAnnotation(ctx context.Context, event string) {
	span := trace.SpanFromContext(ctx)
	span.AddEvent(event)
}

func AddAnnotationWithAttr(ctx context.Context, event string, attr []attribute.KeyValue) {
	span := trace.SpanFromContext(ctx)
	span.AddEvent(event, trace.WithAttributes(attr...))
}

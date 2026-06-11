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
	"net/http"
	"net/url"
	"strings"

	"github.com/google/uuid"
	"go.opentelemetry.io/contrib/detectors/gcp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	semconv "go.opentelemetry.io/otel/semconv/v1.4.0"
	"go.uber.org/zap"
)

type Attributes struct {
	Method    string
	Status    string
	QueryType string
	Keyspace  string
}

type ShutdownFn func(ctx context.Context) error

var (
	attributeKeyDatabase  = attribute.Key("database")
	attributeKeyMethod    = attribute.Key("method")
	attributeKeyStatus    = attribute.Key("status")
	attributeKeyInstance  = attribute.Key("instance")
	attributeKeyQueryType = attribute.Key("query_type")
)

type OTelConfig struct {
	TracerEndpoint     string
	MetricEndpoint     string
	ServiceName        string
	TraceSampleRatio   float64
	OTELEnabled        bool
	Database           string
	Instance           string
	ProjectId          string
	HealthCheckEnabled bool
	HealthCheckEp      string
	ServiceVersion     string
}

type OpenTelemetry struct {
	Config         *OTelConfig
	requestCount   metric.Int64Counter
	requestLatency metric.Int64Histogram
	logger         *zap.Logger
}

// NewOpenTelemetry initializes OpenTelemetry tracing and metrics components.
func NewOpenTelemetry(ctx context.Context, config *OTelConfig, logger *zap.Logger) (*OpenTelemetry, ShutdownFn, error) {
	otelInst := &OpenTelemetry{Config: config, logger: logger}

	if config.OTELEnabled && config.HealthCheckEnabled {
		resp, err := http.Get("http://" + config.HealthCheckEp)
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

	mp, shutdownMp, err := InitMeterProvider(ctx, config, res)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create meter provider: %w", err)
	}
	otel.SetMeterProvider(mp)
	meter := mp.Meter(config.ServiceName)

	otelInst.requestCount, err = meter.Int64Counter(requestCountMetric, metric.WithDescription("Records number of query requests"), metric.WithUnit("1"))
	if err != nil {
		return nil, nil, err
	}
	otelInst.requestLatency, err = meter.Int64Histogram(latencyMetric,
		metric.WithDescription("Records latency for all query operations"),
		metric.WithExplicitBucketBoundaries(0.0, 0.0010, 0.0013, 0.0016, 0.0020, 0.0024, 0.0031, 0.0038, 0.0048, 0.0060,
			0.0075, 0.0093, 0.0116, 0.0146, 0.0182, 0.0227, 0.0284, 0.0355, 0.0444, 0.0555, 0.0694, 0.0867,
			0.1084, 0.1355, 0.1694, 0.2118, 0.2647, 0.3309, 0.4136, 0.5170, 0.6462, 0.8078, 1.0097, 1.2622,
			1.5777, 1.9722, 2.4652, 3.0815, 3.8519, 4.8148, 6.0185, 7.5232, 9.4040, 11.7549, 14.6937, 18.3671,
			22.9589, 28.6986, 35.8732, 44.8416, 56.0519, 70.0649, 87.5812, 109.4764, 136.8456, 171.0569, 213.8212,
			267.2765, 334.0956, 417.6195, 522.0244, 652.5304),
		metric.WithUnit("ms"))
	if err != nil {
		return nil, nil, err
	}

	shutdown := func(ctx context.Context) error {
		err1 := shutdownTp(ctx)
		err2 := shutdownMp(ctx)
		if err1 != nil {
			return err1
		}
		return err2
	}

	return otelInst, shutdown, nil
}

func buildOtelResource(ctx context.Context, config *OTelConfig) *resource.Resource {
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

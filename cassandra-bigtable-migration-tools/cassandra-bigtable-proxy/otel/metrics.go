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
	"errors"
	"github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/global/types"
	"go.opentelemetry.io/otel/metric/noop"
	"time"

	metricexporter "github.com/GoogleCloudPlatform/opentelemetry-operations-go/exporter/metric"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	"go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
)

const (
	requestCountMetric = "bigtable/cassandra_adapter/request_count"
	latencyMetric      = "bigtable/cassandra_adapter/roundtrip_latencies"
)

func InitMeterProvider(ctx context.Context, config *types.OtelConfig, res *resource.Resource) (metric.MeterProvider, ShutdownFn, error) {
	if !config.Enabled {
		return noop.NewMeterProvider(), func(_ context.Context) error { return nil }, nil
	}

	var exporter sdkmetric.Exporter
	var err error

	if config.Metrics.GcpMetricsEnabled {
		exporter, err = metricexporter.New(metricexporter.WithProjectID(config.Traces.ProjectId))
	} else {
		if !isValidEndpoint(config.Metrics.Endpoint) {
			return nil, nil, errors.New("invalid metric endpoint format")
		}
		exporter, err = otlpmetricgrpc.New(ctx, otlpmetricgrpc.WithEndpoint(config.Metrics.Endpoint), otlpmetricgrpc.WithInsecure())
	}

	if err != nil {
		return nil, nil, err
	}
	mp := sdkmetric.NewMeterProvider(
		sdkmetric.WithReader(sdkmetric.NewPeriodicReader(exporter)),
		sdkmetric.WithResource(res),
		sdkmetric.WithView(sdkmetric.NewView(
			sdkmetric.Instrument{Name: "rpc.client.*"},
			sdkmetric.Stream{Aggregation: sdkmetric.AggregationDrop{}},
		)),
	)
	return mp, mp.Shutdown, nil
}

func (o *OpenTelemetry) RecordMetrics(ctx context.Context, startTime time.Time, attrs types.Attributes) {
	a := commonAttributes(attrs)
	o.requestLatency.Record(ctx, time.Since(startTime).Milliseconds(), metric.WithAttributes(a...))
	o.requestCount.Add(ctx, 1, metric.WithAttributes(a...))
}

func commonAttributes(attrs types.Attributes) []attribute.KeyValue {
	return []attribute.KeyValue{
		attributeKeyKeyspace.String(string(attrs.Keyspace)),
		attributeKeyMethod.String(attrs.Method),
		attributeKeyQueryType.String(attrs.QueryType.String()),
		attributeKeyTable.String(string(attrs.Table)),
		attributeKeyStatus.String(attrs.Status),
	}
}

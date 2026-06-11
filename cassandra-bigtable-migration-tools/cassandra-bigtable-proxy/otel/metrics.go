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
	"go.opentelemetry.io/otel/metric/noop"
	"time"

	"github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/global/types"
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

func InitMeterProvider(ctx context.Context, config *OTelConfig, res *resource.Resource) (metric.MeterProvider, ShutdownFn, error) {
	if !config.OTELEnabled {
		return noop.NewMeterProvider(), func(_ context.Context) error { return nil }, nil
	}
	exporter, err := otlpmetricgrpc.New(ctx, otlpmetricgrpc.WithEndpoint(config.MetricEndpoint), otlpmetricgrpc.WithInsecure())
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

func (o *OpenTelemetry) RecordMetrics(ctx context.Context, method string, startTime time.Time, queryType string, keyspace types.Keyspace, err error) {
	if !o.Config.OTELEnabled {
		return
	}
	status := "OK"
	if err != nil {
		status = "failure"
	}
	attrs := Attributes{
		Method:    method,
		Status:    status,
		QueryType: queryType,
		Keyspace:  string(keyspace),
	}
	o.RecordRequestCountMetric(ctx, attrs)
	o.RecordLatencyMetric(ctx, startTime, attrs)
}

func (o *OpenTelemetry) commonAttributes(attrs Attributes) []attribute.KeyValue {
	return []attribute.KeyValue{
		attributeKeyInstance.String(attrs.Keyspace),
		attributeKeyDatabase.String(o.Config.Database),
		attributeKeyMethod.String(attrs.Method),
		attributeKeyQueryType.String(attrs.QueryType),
	}
}

func (o *OpenTelemetry) RecordLatencyMetric(ctx context.Context, startTime time.Time, attrs Attributes) {
	if o.Config.OTELEnabled {
		o.requestLatency.Record(ctx, time.Since(startTime).Milliseconds(), metric.WithAttributes(o.commonAttributes(attrs)...))
	}
}

func (o *OpenTelemetry) RecordRequestCountMetric(ctx context.Context, attrs Attributes) {
	if o.Config.OTELEnabled {
		kv := append(o.commonAttributes(attrs), attributeKeyStatus.String(attrs.Status))
		o.requestCount.Add(ctx, 1, metric.WithAttributes(kv...))
	}
}

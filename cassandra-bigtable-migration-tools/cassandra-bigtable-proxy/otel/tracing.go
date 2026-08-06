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
	"go.opentelemetry.io/otel/trace/noop"

	texporter "github.com/GoogleCloudPlatform/opentelemetry-operations-go/exporter/trace"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

type noOptionsSampler struct {
	sampler sdktrace.Sampler
}

func (s noOptionsSampler) ShouldSample(p sdktrace.SamplingParameters) sdktrace.SamplingResult {
	if p.Name == "options" {
		return sdktrace.SamplingResult{
			Decision:   sdktrace.Drop,
			Attributes: nil,
			Tracestate: trace.SpanContextFromContext(p.ParentContext).TraceState(),
		}
	}
	return s.sampler.ShouldSample(p)
}

func (s noOptionsSampler) Description() string {
	return "NoOptionsSampler(" + s.sampler.Description() + ")"
}

func createTraceProvider(ctx context.Context, config *types.OtelConfig, res *resource.Resource) (trace.TracerProvider, ShutdownFn, error) {
	if !config.Enabled {
		return noop.NewTracerProvider(), func(_ context.Context) error { return nil }, nil
	}

	sampler := sdktrace.Sampler(noOptionsSampler{sampler: sdktrace.ParentBased(sdktrace.TraceIDRatioBased(config.Traces.SamplingRatio))})

	opts := []sdktrace.TracerProviderOption{
		sdktrace.WithResource(res),
		sdktrace.WithSampler(sampler),
	}

	if config.Traces.ProjectId != "" {
		exporter, err := texporter.New(texporter.WithProjectID(config.Traces.ProjectId))
		if err != nil {
			return nil, nil, err
		}
		opts = append(opts, sdktrace.WithBatcher(exporter))
	} else if config.Traces.Endpoint != "" {
		if !isValidEndpoint(config.Traces.Endpoint) {
			return nil, nil, errors.New("invalid Tracer endpoint format")
		}
		exporter, err := otlptracegrpc.New(ctx, otlptracegrpc.WithEndpoint(config.Traces.Endpoint), otlptracegrpc.WithInsecure())
		if err != nil {
			return nil, nil, err
		}
		opts = append(opts, sdktrace.WithBatcher(exporter))
	} else {
		return nil, nil, errors.New("no Tracer endpoint or project id provided")
	}
	result := sdktrace.NewTracerProvider(opts...)
	return result, result.Shutdown, nil
}

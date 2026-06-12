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
	"go.opentelemetry.io/otel/trace/noop"

	texporter "github.com/GoogleCloudPlatform/opentelemetry-operations-go/exporter/trace"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

func createTraceProvider(ctx context.Context, config *OTelConfig, res *resource.Resource) (trace.TracerProvider, ShutdownFn, error) {
	if !config.OTELEnabled {
		return noop.NewTracerProvider(), func(_ context.Context) error { return nil }, nil
	}

	opts := []sdktrace.TracerProviderOption{
		sdktrace.WithResource(res),
		sdktrace.WithSampler(sdktrace.ParentBased(sdktrace.TraceIDRatioBased(config.TraceSampleRatio))),
	}

	if config.ProjectId != "" {
		exporter, err := texporter.New(texporter.WithProjectID(config.ProjectId))
		if err != nil {
			return nil, nil, err
		}
		opts = append(opts, sdktrace.WithBatcher(exporter))
	} else if config.TracerEndpoint != "" {
		if !isValidEndpoint(config.TracerEndpoint) {
			return nil, nil, errors.New("invalid Tracer endpoint format")
		}
		exporter, err := otlptracegrpc.New(ctx, otlptracegrpc.WithEndpoint(config.TracerEndpoint), otlptracegrpc.WithInsecure())
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

/*
Copyright 2020 The Knative Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package handler

import (
	"context"

	pkgmetrics "knative.dev/pkg/metrics"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

var (
	// Meter is the OpenTelemetry meter used for creating metric instruments
	meter = otel.GetMeterProvider().Meter("knative.dev/serving/pkg/activator")

	requestConcurrency  metric.Float64UpDownCounter
	requestCount        metric.Int64Counter
	responseTimeInMsecM metric.Float64Histogram

	// The following boundaries are based on the previous OpenCensus configuration
	// NOTE: 0 should not be used as boundary.
	defaultLatencyDistribution = []float64{5, 10, 20, 40, 60, 80, 100, 150, 200, 250, 300, 350, 400, 450, 500, 600, 700, 800, 900, 1000, 2000, 5000, 10000, 20000, 50000, 100000}
)

func init() {
	register()
}

func register() {
	var err error

	// Create instruments with the same names, descriptions, and units as the OpenCensus metrics
	requestConcurrency, err = meter.Float64UpDownCounter(
		"request_concurrency",
		metric.WithDescription("Concurrent requests that are routed to Activator"),
		metric.WithUnit("{count}"))
	if err != nil {
		panic(err)
	}

	requestCount, err = meter.Int64Counter(
		"request_count",
		metric.WithDescription("The number of requests that are routed to Activator"),
		metric.WithUnit("{count}"))
	if err != nil {
		panic(err)
	}

	responseTimeInMsecM, err = meter.Float64Histogram(
		"request_latencies",
		metric.WithDescription("The response time in millisecond"),
		metric.WithUnit("ms"),
		metric.WithExplicitBucketBoundaries(defaultLatencyDistribution...))
	if err != nil {
		panic(err)
	}
}

// RecordRequestMetrics records metrics for a request
func RecordRequestMetrics(ctx context.Context, responseCode int, latencyMs float64) {
	// Create attribute set similar to what would be used with OpenCensus
	attrs := attribute.NewSet(
		attribute.Int("response_code", responseCode),
		attribute.String("response_code_class", pkgmetrics.ResponseCodeClass(responseCode)),
	)

	responseTime := metric.WithAttributeSet(attrs)
	requestCount.Add(ctx, 1, responseTime)
	responseTimeInMsecM.Record(ctx, latencyMs, responseTime)
}

// RecordConcurrencyMetrics records the current concurrency value
func RecordConcurrencyMetrics(ctx context.Context, concurrency float64) {
	// We're using a gauge-like pattern here, so we need to set the absolute value
	// This is different from the OpenCensus approach which used LastValue aggregation
	requestConcurrency.Add(ctx, concurrency)
}

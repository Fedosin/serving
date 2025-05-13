/*
Copyright 2019 The Knative Authors

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

package queue

import (
	"context"
	"net/http"
	"strconv"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	netheader "knative.dev/networking/pkg/http/header"
	pkghttp "knative.dev/serving/pkg/http"
	"knative.dev/serving/pkg/metrics"
)

var (
	// NOTE: We're maintaining the same bucket boundaries that were previously used with OpenCensus
	// https://github.com/census-ecosystem/opencensus-go-exporter-stackdriver/issues/98
	defaultLatencyDistribution = []float64{
		5, 10, 20, 40, 60, 80, 100, 150, 200, 250, 300, 350, 400, 450, 500, 600,
		700, 800, 900, 1000, 2000, 5000, 10000, 20000, 50000, 100000,
	}

	// Metric instruments
	requestCountM          metric.Int64Counter
	responseTimeInMsecM    metric.Float64Histogram
	appRequestCountM       metric.Int64Counter
	appResponseTimeInMsecM metric.Float64Histogram
	queueDepthM            metric.Int64ObservableGauge

	// Metric instrument initialization function
	initMetrics = func() {
		meter := otel.GetMeterProvider().Meter("knative.dev/serving/pkg/queue")

		var err error
		requestCountM, err = meter.Int64Counter(
			"request_count",
			metric.WithDescription("The number of requests that are routed to queue-proxy"),
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

		appRequestCountM, err = meter.Int64Counter(
			"app_request_count",
			metric.WithDescription("The number of requests that are routed to user-container"),
			metric.WithUnit("{count}"))
		if err != nil {
			panic(err)
		}

		appResponseTimeInMsecM, err = meter.Float64Histogram(
			"app_request_latencies",
			metric.WithDescription("The response time in millisecond"),
			metric.WithUnit("ms"),
			metric.WithExplicitBucketBoundaries(defaultLatencyDistribution...))
		if err != nil {
			panic(err)
		}

		queueDepthM, err = meter.Int64ObservableGauge(
			"queue_depth",
			metric.WithDescription("The current number of items in the serving and waiting queue, or not reported if unlimited concurrency."),
			metric.WithUnit("{count}"))
		if err != nil {
			panic(err)
		}
	}
)

func init() {
	initMetrics()
}

type requestMetricsHandler struct {
	next     http.Handler
	statsCtx context.Context
}

type appRequestMetricsHandler struct {
	next     http.Handler
	statsCtx context.Context
	breaker  *Breaker
}

// NewRequestMetricsHandler creates an http.Handler that emits request metrics.
func NewRequestMetricsHandler(next http.Handler,
	ns, service, config, rev, pod string,
) (http.Handler, error) {
	ctx, err := metrics.PodRevisionContext(pod, "queue-proxy", ns, service, config, rev)
	if err != nil {
		return nil, err
	}

	return &requestMetricsHandler{
		next:     next,
		statsCtx: ctx,
	}, nil
}

func (h *requestMetricsHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	rr := pkghttp.NewResponseRecorder(w, http.StatusOK)
	startTime := time.Now()

	defer func() {
		// Filter probe requests for revision metrics.
		if netheader.IsProbe(r) {
			return
		}

		// If ServeHTTP panics, recover, record the failure and panic again.
		err := recover()
		latency := time.Since(startTime)
		routeTag := GetRouteTagNameFromRequest(r)

		// Create attributes
		attrs := []attribute.KeyValue{
			metrics.PodKey.String(getPodName(h.statsCtx)),
			metrics.ContainerKey.String(getContainerName(h.statsCtx)),
			metrics.RouteTagKey.String(routeTag),
		}

		responseCode := rr.ResponseCode
		if err != nil {
			responseCode = http.StatusInternalServerError
		}

		// Add response code attributes
		attrs = append(attrs,
			metrics.ResponseCodeKey.String(strconv.Itoa(responseCode)),
			metrics.ResponseCodeClassKey.String(responseCodeClass(responseCode)))

		requestCountM.Add(h.statsCtx, 1, metric.WithAttributes(attrs...))
		responseTimeInMsecM.Record(h.statsCtx, float64(latency.Milliseconds()), metric.WithAttributes(attrs...))

		if err != nil {
			panic(err)
		}
	}()

	h.next.ServeHTTP(rr, r)
}

// NewAppRequestMetricsHandler creates an http.Handler that emits request metrics.
func NewAppRequestMetricsHandler(next http.Handler, b *Breaker,
	ns, service, config, rev, pod string,
) (http.Handler, error) {
	ctx, err := metrics.PodRevisionContext(pod, "queue-proxy", ns, service, config, rev)
	if err != nil {
		return nil, err
	}

	handler := &appRequestMetricsHandler{
		next:     next,
		statsCtx: ctx,
		breaker:  b,
	}

	// Register callback for queue depth metric if breaker is available
	if b != nil {
		meter := otel.GetMeterProvider().Meter("knative.dev/serving/pkg/queue")
		_, err = meter.RegisterCallback(
			func(_ context.Context, o metric.Observer) error {
				attrs := []attribute.KeyValue{
					metrics.PodKey.String(getPodName(handler.statsCtx)),
					metrics.ContainerKey.String(getContainerName(handler.statsCtx)),
				}
				o.ObserveInt64(queueDepthM, int64(b.InFlight()), metric.WithAttributes(attrs...))
				return nil
			},
			queueDepthM,
		)
		if err != nil {
			return nil, err
		}
	}

	return handler, nil
}

func (h *appRequestMetricsHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	rr := pkghttp.NewResponseRecorder(w, http.StatusOK)
	startTime := time.Now()

	defer func() {
		// Filter probe requests for revision metrics.
		if netheader.IsProbe(r) {
			return
		}

		// If ServeHTTP panics, recover, record the failure and panic again.
		err := recover()
		latency := time.Since(startTime)

		// Create attributes
		attrs := []attribute.KeyValue{
			metrics.PodKey.String(getPodName(h.statsCtx)),
			metrics.ContainerKey.String(getContainerName(h.statsCtx)),
		}

		responseCode := rr.ResponseCode
		if err != nil {
			responseCode = http.StatusInternalServerError
		}

		// Add response code attributes
		attrs = append(attrs,
			metrics.ResponseCodeKey.String(strconv.Itoa(responseCode)),
			metrics.ResponseCodeClassKey.String(responseCodeClass(responseCode)))

		appRequestCountM.Add(h.statsCtx, 1, metric.WithAttributes(attrs...))
		appResponseTimeInMsecM.Record(h.statsCtx, float64(latency.Milliseconds()), metric.WithAttributes(attrs...))

		if err != nil {
			panic(err)
		}
	}()
	h.next.ServeHTTP(rr, r)
}

const (
	defaultTagName   = "DEFAULT"
	undefinedTagName = "UNDEFINED"
	disabledTagName  = "DISABLED"
)

// GetRouteTagNameFromRequest extracts the value of the tag header from http.Request
func GetRouteTagNameFromRequest(r *http.Request) string {
	name := r.Header.Get(netheader.RouteTagKey)
	isDefaultRoute := r.Header.Get(netheader.DefaultRouteKey)

	if name == "" {
		if isDefaultRoute == "" {
			// If there are no tag header and no `Knative-Serving-Default-Route` header,
			// it means that the tag header based routing is disabled, so the tag value is set to `disabled`.
			return disabledTagName
		}
		// If there is no tag header, just returns "default".
		return defaultTagName
	} else if isDefaultRoute == "true" {
		// If there is a tag header with not-empty string and the request is routed via the default route,
		// returns "undefined".
		return undefinedTagName
	}
	// Otherwise, returns the value of the tag header.
	return name
}

// Helper functions for attribute extraction

// attributesKey is the context key for storing attributes
type attributesKey struct{}

// getAttributes retrieves the attributes from the context
func getAttributes(ctx context.Context) []attribute.KeyValue {
	if attrs, ok := ctx.Value(attributesKey{}).([]attribute.KeyValue); ok {
		return attrs
	}
	return []attribute.KeyValue{}
}

// Helper function to extract the pod name from the context
func getPodName(ctx context.Context) string {
	attrs := getAttributes(ctx)
	for _, attr := range attrs {
		if attr.Key == metrics.PodKey {
			return attr.Value.AsString()
		}
	}
	return ""
}

// Helper function to extract the container name from the context
func getContainerName(ctx context.Context) string {
	attrs := getAttributes(ctx)
	for _, attr := range attrs {
		if attr.Key == metrics.ContainerKey {
			return attr.Value.AsString()
		}
	}
	return ""
}

// responseCodeClass converts response code to a string of response code class.
// e.g. The response code class is "5xx" for response code 503.
func responseCodeClass(responseCode int) string {
	// Get the hundreds digit of the response code and concatenate "xx".
	return strconv.Itoa(responseCode/100) + "xx"
}

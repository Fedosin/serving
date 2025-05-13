/*
Copyright 2021 The Knative Authors

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
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"knative.dev/pkg/logging"
	rtesting "knative.dev/pkg/reconciler/testing"
	"knative.dev/pkg/tracing/config"
	activatorconfig "knative.dev/serving/pkg/activator/config"

	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

func TestTracingHandler(t *testing.T) {
	tests := []struct {
		name           string
		tracingEnabled bool
	}{{
		name:           "enabled",
		tracingEnabled: true,
	}, {
		name:           "disabled",
		tracingEnabled: false,
	}}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx, cancel, _ := rtesting.SetupFakeContextWithCancel(t)
			defer cancel()

			baseHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {})
			handler := NewTracingHandler(baseHandler)

			resp := httptest.NewRecorder()
			req := httptest.NewRequest(http.MethodPost, "http://example.com", nil)
			const (
				traceID = "821e0d50d931235a5ba3fa42eddddd8f"
				spanID  = "b3bd5e1c4318c78a"
			)
			req.Header.Set("traceparent", traceparentHeader(traceID, spanID))

			cm := tracingConfig(test.tracingEnabled)
			_, err := config.NewTracingConfigFromConfigMap(cm)
			if err != nil {
				t.Fatal("Failed to parse tracing config", err)
			}

			configStore := activatorconfig.NewStore(logging.FromContext(ctx))
			configStore.OnConfigChanged(cm)
			ctx = configStore.ToContext(ctx)

			// Set up in-memory exporter for OpenTelemetry spans.
			exporter := newInMemoryExporter()
			tp := sdktrace.NewTracerProvider(
				sdktrace.WithSpanProcessor(sdktrace.NewSimpleSpanProcessor(exporter)),
			)
			otel.SetTracerProvider(tp)
			t.Cleanup(func() {
				_ = tp.Shutdown(context.Background())
			})

			handler.ServeHTTP(resp, req.WithContext(ctx))

			spans := exporter.Flush()

			if test.tracingEnabled {
				if len(spans) != 1 {
					t.Errorf("Got %d spans, expected 1: spans = %v", len(spans), spans)
				}
				if got := spans[0].SpanContext().TraceID().String(); got != traceID {
					t.Errorf("spans[0].TraceID = %s, want %s", got, traceID)
				}
			} else if len(spans) != 0 {
				t.Errorf("Got %d spans, expected 0: spans = %v", len(spans), spans)
			}
		})
	}
}

func tracingConfig(enabled bool) *corev1.ConfigMap {
	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name: config.ConfigName,
		},
		Data: map[string]string{
			"backend": "none",
		},
	}
	if enabled {
		cm.Data["backend"] = "zipkin"
		cm.Data["zipkin-endpoint"] = "foo.bar"
		cm.Data["debug"] = "true"
	}
	return cm
}

// ----------------------- helpers -----------------------

// inMemoryExporter is a simple SpanExporter that stores spans in memory for inspection.
type inMemoryExporter struct {
	mu    sync.Mutex
	spans []sdktrace.ReadOnlySpan
}

func newInMemoryExporter() *inMemoryExporter { return &inMemoryExporter{} }

func (e *inMemoryExporter) ExportSpans(_ context.Context, spans []sdktrace.ReadOnlySpan) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.spans = append(e.spans, spans...)
	return nil
}

func (e *inMemoryExporter) Shutdown(_ context.Context) error { return nil }

func (e *inMemoryExporter) Flush() []sdktrace.ReadOnlySpan {
	e.mu.Lock()
	defer e.mu.Unlock()
	out := make([]sdktrace.ReadOnlySpan, len(e.spans))
	copy(out, e.spans)
	return out
}

// traceparentHeader returns a formatted W3C traceparent header value for the
// given trace and span IDs.
func traceparentHeader(traceID, spanID string) string {
	return fmt.Sprintf("00-%s-%s-01", traceID, spanID)
}

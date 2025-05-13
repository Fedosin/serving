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

package metrics

import (
	"context"
	"fmt"
	"math/rand"
	"strconv"
	"testing"

	_ "knative.dev/pkg/metrics/testing"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/resource"
)

// We'll simplify the test and focus on checking the context attributes and resources

func TestContexts(t *testing.T) {
	tests := []struct {
		name         string
		ctx          context.Context
		wantAttrs    []attribute.KeyValue
		wantResource *resource.Resource
	}{{
		name: "pod context",
		ctx: mustCtx(t, func() (context.Context, error) {
			return podContext("testpod", "testcontainer")
		}),
		wantAttrs: []attribute.KeyValue{
			PodKey.String("testpod"),
			ContainerKey.String("testcontainer"),
		},
	}, {
		name: "revision context",
		ctx: purge(t, func() context.Context {
			return RevisionContext("testns", "testsvc", "testcfg", "testrev")
		}),
		wantAttrs: []attribute.KeyValue{},
		wantResource: resource.NewWithAttributes(
			"",
			attribute.String("service.name", "knative_revision"),
			attribute.String(LabelNamespaceName, "testns"),
			attribute.String(LabelServiceName, "testsvc"),
			attribute.String(LabelConfigurationName, "testcfg"),
			attribute.String(LabelRevisionName, "testrev"),
		),
	}, {
		name: "revision context (empty svc)",
		ctx: purge(t, func() context.Context {
			return RevisionContext("testns", "", "testcfg", "testrev")
		}),
		wantAttrs: []attribute.KeyValue{},
		wantResource: resource.NewWithAttributes(
			"",
			attribute.String("service.name", "knative_revision"),
			attribute.String(LabelNamespaceName, "testns"),
			attribute.String(LabelServiceName, ValueUnknown),
			attribute.String(LabelConfigurationName, "testcfg"),
			attribute.String(LabelRevisionName, "testrev"),
		),
	}, {
		name: "pod revision context",
		ctx: mustCtx(t, func() (context.Context, error) {
			return PodRevisionContext("testpod", "testcontainer", "testns", "testsvc", "testcfg", "testrev")
		}),
		wantAttrs: []attribute.KeyValue{
			PodKey.String("testpod"),
			ContainerKey.String("testcontainer"),
		},
		wantResource: resource.NewWithAttributes(
			"",
			attribute.String("service.name", "knative_revision"),
			attribute.String(LabelNamespaceName, "testns"),
			attribute.String(LabelServiceName, "testsvc"),
			attribute.String(LabelConfigurationName, "testcfg"),
			attribute.String(LabelRevisionName, "testrev"),
		),
	}, {
		name: "pod revision context (empty svc)",
		ctx: mustCtx(t, func() (context.Context, error) {
			return PodRevisionContext("testpod", "testcontainer", "testns", "", "testcfg", "testrev")
		}),
		wantAttrs: []attribute.KeyValue{
			PodKey.String("testpod"),
			ContainerKey.String("testcontainer"),
		},
		wantResource: resource.NewWithAttributes(
			"",
			attribute.String("service.name", "knative_revision"),
			attribute.String(LabelNamespaceName, "testns"),
			attribute.String(LabelServiceName, ValueUnknown),
			attribute.String(LabelConfigurationName, "testcfg"),
			attribute.String(LabelRevisionName, "testrev"),
		),
	}, {
		name: "pod revision context (empty svc)",
		ctx: mustCtx(t, func() (context.Context, error) {
			return PodRevisionContext("testpod", "testcontainer", "testns", "", "testcfg", "testrev")
		}),
		wantAttrs: []attribute.KeyValue{
			PodKey.String("testpod"),
			ContainerKey.String("testcontainer"),
		},
		wantResource: resource.NewWithAttributes(
			"",
			attribute.String("service.name", "knative_revision"),
			attribute.String(LabelNamespaceName, "testns"),
			attribute.String(LabelServiceName, ValueUnknown),
			attribute.String(LabelConfigurationName, "testcfg"),
			attribute.String(LabelRevisionName, "testrev"),
		),
	}, {
		name: "pod context augmented with revision",
		ctx: mustCtx(t, func() (context.Context, error) {
			ctx, err := podContext("testpod", "testcontainer")
			if err != nil {
				return ctx, err
			}
			return augmentWithRevision(ctx, "testns", "testsvc", "testcfg", "testrev"), nil
		}),
		wantAttrs: []attribute.KeyValue{
			PodKey.String("testpod"),
			ContainerKey.String("testcontainer"),
		},
		wantResource: resource.NewWithAttributes(
			"",
			attribute.String("service.name", "knative_revision"),
			attribute.String(LabelNamespaceName, "testns"),
			attribute.String(LabelServiceName, "testsvc"),
			attribute.String(LabelConfigurationName, "testcfg"),
			attribute.String(LabelRevisionName, "testrev"),
		),
	}, {
		name: "pod revision context augmented with response",
		ctx: mustCtx(t, func() (context.Context, error) {
			ctx, err := PodRevisionContext("testpod", "testcontainer", "testns", "testsvc", "testcfg", "testrev")
			return AugmentWithResponse(ctx, 200), err
		}),
		wantAttrs: []attribute.KeyValue{
			PodKey.String("testpod"),
			ContainerKey.String("testcontainer"),
			ResponseCodeKey.String("200"),
			ResponseCodeClassKey.String("2xx"),
		},
		wantResource: resource.NewWithAttributes(
			"",
			attribute.String("service.name", "knative_revision"),
			attribute.String(LabelNamespaceName, "testns"),
			attribute.String(LabelServiceName, "testsvc"),
			attribute.String(LabelConfigurationName, "testcfg"),
			attribute.String(LabelRevisionName, "testrev"),
		),
	}}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			// Extract the attributes and resource from the context
			attrs := getAttributes(test.ctx)
			res := GetResource(test.ctx)

			// Verify attributes
			if len(test.wantAttrs) != len(attrs) {
				t.Errorf("Got %d attributes, want %d", len(attrs), len(test.wantAttrs))
			}
			for _, want := range test.wantAttrs {
				found := false
				for _, got := range attrs {
					if want.Key == got.Key && want.Value.AsString() == got.Value.AsString() {
						found = true
						break
					}
				}
				if !found {
					t.Errorf("Did not find attribute %v in %v", want, attrs)
				}
			}

			// Verify resource
			if test.wantResource != nil {
				if res == nil {
					t.Error("Expected resource, got nil")
				} else {
					// Compare resources
					wantAttrs := test.wantResource.Attributes()
					gotAttrs := res.Attributes()
					if len(wantAttrs) != len(gotAttrs) {
						t.Errorf("Resource attributes count mismatch: got %d, want %d", len(gotAttrs), len(wantAttrs))
					}
					for _, want := range wantAttrs {
						found := false
						for _, got := range gotAttrs {
							if want.Key == got.Key && want.Value.AsString() == got.Value.AsString() {
								found = true
								break
							}
						}
						if !found {
							t.Errorf("Did not find resource attribute %v in %v", want, gotAttrs)
						}
					}
				}
			}
		})
	}
}

func BenchmarkPodRevisionContext(b *testing.B) {
	// test with 1 (always hits cache),  1024 (25% load), 4095 (always hits cache, but at capacity),
	// 16k (often misses the cache) and 409600  (practically always misses cache)
	for _, revisions := range []int{1, 1024, 4095, 0xFFFF, 409600} {
		b.Run(fmt.Sprintf("sequential-%d-revisions", revisions), func(b *testing.B) {
			contextCache.Purge()
			for range b.N {
				rev := "name" + strconv.Itoa(rand.Intn(revisions))
				PodRevisionContext("pod", "container", "ns", "svc", "cfg", rev)
			}
		})

		b.Run(fmt.Sprintf("parallel-%d-revisions", revisions), func(b *testing.B) {
			contextCache.Purge()

			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					rev := "name" + strconv.Itoa(rand.Intn(revisions))
					PodRevisionContext("pod", "container", "ns", "svc", "cfg", rev)
				}
			})
		})
	}
}

func mustCtx(t *testing.T, f func() (context.Context, error)) context.Context {
	t.Helper()

	// Force a way around the cache.
	contextCache.Purge()

	ctx, err := f()
	if err != nil {
		t.Fatal("Failed to create a new context:", err)
	}
	return ctx
}

func purge(t *testing.T, f func() context.Context) context.Context {
	t.Helper()

	// Force a way around the cache.
	contextCache.Purge()

	ctx := f()
	return ctx
}

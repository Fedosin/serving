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
	"strconv"

	lru "github.com/hashicorp/golang-lru"
	"k8s.io/apimachinery/pkg/types"
	"knative.dev/pkg/metrics/metricskey"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/resource"
)

// contextCache stores the metrics recorder contexts in an LRU cache.
// Hashicorp LRU cache is synchronized.
var contextCache *lru.Cache

// This is a fairly arbitrary number but we want it to be higher than the
// number of active revisions a single activator might be handling, to avoid
// churning the cache, without being so much higher that it causes an
// (effective) memory leak.
// The contents of the cache are quite small, so we can err on the high side.
const lruCacheSize = 4096

func init() {
	// The only possible error is when cache size is not positive.
	contextCache, _ = lru.New(lruCacheSize)
}

func valueOrUnknown(v string) string {
	if v != "" {
		return v
	}
	return metricskey.ValueUnknown
}

// RevisionContext generates a new base metric reporting context containing
// the respective revision specific tags.
func RevisionContext(ns, svc, cfg, rev string) context.Context {
	key := types.NamespacedName{Namespace: ns, Name: rev}
	if ctx, ok := contextCache.Get(key); ok {
		return ctx.(context.Context)
	}

	ctx := augmentWithRevision(context.Background(), ns, svc, cfg, rev)
	contextCache.Add(key, ctx)

	return ctx
}

type podCtx struct {
	pod, container string
}

// podContext generates a new base metric reporting context containing
// the respective pod specific tags.
func podContext(pod, container string) (context.Context, error) {
	key := podCtx{pod: pod, container: container}
	if ctx, ok := contextCache.Get(key); ok {
		return ctx.(context.Context), nil
	}

	attrs := []attribute.KeyValue{
		PodKey.String(pod),
		ContainerKey.String(container),
	}
	ctx := context.WithValue(context.Background(), attributesKey{}, attrs)

	contextCache.Add(key, ctx)
	return ctx, nil
}

type podRevisionCtx struct {
	pod      podCtx
	revision types.NamespacedName
}

// PodRevisionContext generates a new base metric reporting context containing
// the respective pod and revision specific tags.
func PodRevisionContext(pod, container, ns, svc, cfg, rev string) (context.Context, error) {
	key := podRevisionCtx{
		pod:      podCtx{pod: pod, container: container},
		revision: types.NamespacedName{Namespace: ns, Name: rev},
	}

	if ctx, ok := contextCache.Get(key); ok {
		return ctx.(context.Context), nil
	}

	ctx, err := podContext(pod, container)
	if err != nil {
		return ctx, err
	}

	ctx = augmentWithRevision(ctx, ns, svc, cfg, rev)
	contextCache.Add(key, ctx)
	return ctx, nil
}

// attributesKey is the context key for storing attributes
type attributesKey struct{}

// resourceKey is the context key for storing resource
type resourceKey struct{}

// augmentWithRevision augments the given context with a knative_revision resource.
func augmentWithRevision(baseCtx context.Context, ns, svc, cfg, rev string) context.Context {
	r := resource.NewWithAttributes(
		"", // No schema URL needed
		attribute.String("service.name", ResourceTypeKnativeRevision),
		attribute.String(LabelNamespaceName, ns),
		attribute.String(LabelServiceName, valueOrUnknown(svc)),
		attribute.String(LabelConfigurationName, cfg),
		attribute.String(LabelRevisionName, rev),
	)
	return context.WithValue(baseCtx, resourceKey{}, r)
}

// AugmentWithResponse augments the given context with response-code specific tags.
func AugmentWithResponse(baseCtx context.Context, responseCode int) context.Context {
	attrs := getAttributes(baseCtx)
	attrs = append(attrs,
		ResponseCodeKey.String(strconv.Itoa(responseCode)),
		ResponseCodeClassKey.String(responseCodeClass(responseCode)),
	)
	return context.WithValue(baseCtx, attributesKey{}, attrs)
}

// AugmentWithResponseAndRouteTag augments the given context with response-code and route-tag specific tags.
func AugmentWithResponseAndRouteTag(baseCtx context.Context, responseCode int, routeTag string) context.Context {
	attrs := getAttributes(baseCtx)
	attrs = append(attrs,
		ResponseCodeKey.String(strconv.Itoa(responseCode)),
		ResponseCodeClassKey.String(responseCodeClass(responseCode)),
		RouteTagKey.String(routeTag),
	)
	return context.WithValue(baseCtx, attributesKey{}, attrs)
}

// getAttributes retrieves the attributes from the context
func getAttributes(ctx context.Context) []attribute.KeyValue {
	if attrs, ok := ctx.Value(attributesKey{}).([]attribute.KeyValue); ok {
		return attrs
	}
	return []attribute.KeyValue{}
}

// GetResource retrieves the resource from the context
func GetResource(ctx context.Context) *resource.Resource {
	if res, ok := ctx.Value(resourceKey{}).(*resource.Resource); ok {
		return res
	}
	return nil
}

// responseCodeClass converts response code to a string of response code class.
// e.g. The response code class is "5xx" for response code 503.
func responseCodeClass(responseCode int) string {
	// Get the hundreds digit of the response code and concatenate "xx".
	return strconv.Itoa(responseCode/100) + "xx"
}

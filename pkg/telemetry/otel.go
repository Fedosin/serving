package telemetry

import (
    "context"
    "time"

    "go.opentelemetry.io/otel"
    "go.opentelemetry.io/otel/attribute"
    "go.opentelemetry.io/otel/sdk/resource"
    semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
    sdktrace "go.opentelemetry.io/otel/sdk/trace"
    "go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
)

// InstallTracing installs tracing for a service
func InstallTracing(ctx context.Context, svc, pod, podIP string,
                    sample float64, collector string) (shutdown func(ctx context.Context) error, err error) {
    res, _ := resource.New(ctx,
        resource.WithAttributes(
            semconv.ServiceNameKey.String(svc),
            attribute.String("k8s.pod.name", pod),
            attribute.String("k8s.pod.ip", podIP),
        ),
        resource.WithFromEnv(),
        resource.WithTelemetrySDK(),
    )

    exp, err := otlptracegrpc.New(ctx,
        otlptracegrpc.WithEndpoint(collector),
        otlptracegrpc.WithInsecure(),
    )
    if err != nil {
        return nil, err
    }

    tp := sdktrace.NewTracerProvider(
        sdktrace.WithSpanProcessor(
            sdktrace.NewBatchSpanProcessor(exp,
                sdktrace.WithMaxExportBatchSize(512),
                sdktrace.WithBatchTimeout(3*time.Second))),
        sdktrace.WithResource(res),
        sdktrace.WithSampler(sdktrace.ParentBased(
            sdktrace.TraceIDRatioBased(sample))),
    )

    otel.SetTracerProvider(tp)

    return tp.Shutdown, nil
}

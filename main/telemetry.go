package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/semconv/v1.37.0"
)

func startTelemetry() func() {
	if os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT") == "" && os.Getenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT") == "" {
		return func() {}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	exporter, err := otlptracehttp.New(ctx)
	if err != nil {
		fmt.Fprintf(os.Stderr, "xray: OpenTelemetry exporter unavailable: %v\n", err)
		return func() {}
	}
	attrs := []attribute.KeyValue{semconv.ServiceName("xray")}
	if nodeID := os.Getenv("XCONNECT_NODE_ID"); nodeID != "" {
		attrs = append(attrs, attribute.String("xconnect.node.id", nodeID))
	}
	res, err := resource.New(ctx, resource.WithTelemetrySDK(), resource.WithAttributes(attrs...))
	if err != nil {
		_ = exporter.Shutdown(ctx)
		fmt.Fprintf(os.Stderr, "xray: OpenTelemetry resource unavailable: %v\n", err)
		return func() {}
	}
	provider := sdktrace.NewTracerProvider(
		sdktrace.WithResource(res),
		sdktrace.WithBatcher(exporter,
			sdktrace.WithMaxQueueSize(2048),
			sdktrace.WithBatchTimeout(5*time.Second),
			sdktrace.WithExportTimeout(3*time.Second),
		),
	)
	otel.SetTracerProvider(provider)
	otel.SetTextMapPropagator(propagation.TraceContext{})
	return func() {
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer shutdownCancel()
		if err := provider.Shutdown(shutdownCtx); err != nil {
			fmt.Fprintf(os.Stderr, "xray: OpenTelemetry shutdown failed: %v\n", err)
		}
	}
}

package oteltest

import (
	"go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

type InMemoryExporterWrapperImpl struct {
	rawSpanExporter trace.SpanExporter
	getSpans        func() tracetest.SpanStubs
}

func (i InMemoryExporterWrapperImpl) RawSpanExporter() trace.SpanExporter {
	return i.rawSpanExporter
}

func (i InMemoryExporterWrapperImpl) GetSpans() tracetest.SpanStubs {
	return i.getSpans()
}

// NewInMemoryExporterWrapper - This function is create a in-memory exporter for OpenTelemetry tracing.
// By using this , you can test your application's tracing functionality
// without sending data to a real tracing backend.
// without sending data to a real tracing backend.
// func NewInMemoryExporterWrapper() otel.ExporterWrapper {
func NewInMemoryExporterWrapper() *InMemoryExporterWrapperImpl {
	exporter := tracetest.NewInMemoryExporter()
	exporter.GetSpans()
	return &InMemoryExporterWrapperImpl{
		rawSpanExporter: exporter,
		getSpans:        exporter.GetSpans,
	}
}

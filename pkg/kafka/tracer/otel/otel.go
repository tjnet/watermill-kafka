// Package otel provides a tracer that acts as a open telemetry tracer.
package otel

import (
	"context"
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/ThreeDotsLabs/watermill-kafka/v2/pkg/kafka/tracer"
	sdkos "github.com/ThreeDotsLabs/watermill-kafka/v2/pkg/os"
	"go.opentelemetry.io/contrib/instrumentation/github.com/Shopify/sarama/otelsarama"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.7.0"
	"google.golang.org/grpc/credentials"
)

// TODO: refactor wrapper
// https://github.com/Zh3Wang/go-micro-opentelemetry/blob/4d71d375c09f341e21c793d5f4b467b587530f5b/opentelemetry.go#L16

// tracer is goroutine safe. we can share tracer in package scope.
var oteltracer = otel.Tracer("watermill-kafka/pkg/kafka/tracer/otel")

// usage (japanese)
// https://zenn.dev/ww24/articles/beae98be198c94
// https://christina04.hatenablog.com/entry/opentelemetry-in-go
// usage (english)
// https://gethelios.dev/blog/golang-distributed-tracing-opentelemetry-based-observability/

// mockTracerWrapper has moduleA
// OtelTracerWrapper has moduleA
// using moduleA, we can do TraceSendStart

type ExporterFactory func(ctx context.Context, config *OTELConfig) (ExporterWrapper, error)

type OtelTracerWrapper struct {
	config                 *OTELConfig
	exporterWrapperFactory ExporterFactory

	tracerProvider       *sdktrace.TracerProvider
	tracerProviderOption sdktrace.TracerProviderOption
}

func (o *OtelTracerWrapper) WrapSyncProducer(saramaConfig *sarama.Config, producer sarama.SyncProducer) sarama.SyncProducer {
	return otelsarama.WrapSyncProducer(saramaConfig, producer)
}

// TODO: set fist args to ctx
func (o *OtelTracerWrapper) TraceSendStart(ctx context.Context, message *sarama.ProducerMessage) (func(), error) {
	tp, err := o.traceProvider(ctx)
	if err != nil {
		return func() {

		}, err
	}
	otel.SetTracerProvider(tp)
	otel.GetTextMapPropagator().Inject(ctx, otelsarama.NewProducerMessageCarrier(message))
	return func() {

	}, nil
}

type ProviderConfig struct {
	exporter sdktrace.SpanExporter
}

type ProviderOptionError struct {
	Field string
	Err   error
}

func (p *ProviderOptionError) Error() string {
	return fmt.Sprintf("%s: %s", p.Field, p.Err.Error())
}

func (o *OtelTracerWrapper) traceProvider(ctx context.Context) (*sdktrace.TracerProvider, error) {
	exporterWrapper, err := o.exporterWrapperFactory(ctx, o.config)

	if err != nil {
		return nil, err
	}
	tp := sdktrace.NewTracerProvider(
		// Always be sure to batch in production.
		sdktrace.WithBatcher(exporterWrapper.RawSpanExporter()),
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
		// Record information about this application in an Resource
		sdktrace.WithResource(resource.NewWithAttributes(
			semconv.SchemaURL,
			semconv.ServiceNameKey.String(o.config.SERVICENAME),
			attribute.String("host", sdkos.GetHostname()),
			attribute.String("version", o.config.VERSION),
		)),
	)
	return tp, nil
}

func (o *OtelTracerWrapper) security() otlptracegrpc.Option {
	if o.config.UseTLS {
		return otlptracegrpc.WithTLSCredentials(credentials.NewClientTLSFromCert(nil, ""))
	}
	return otlptracegrpc.WithInsecure()
}

// TODO: override exporter using inject function like this. o.exporter = func(ctx){...}
// returns a SpanExporter to sync spans to otel provider.
// for moore details, see following URLs.
func standardExporter(ctx context.Context, config *OTELConfig) (ExporterWrapper, error) {
	// see https://github.com/ehazlett/sysinfo/blob/6539d4171250bf54847f9ed5d315e84daa9c1191/tracing.go#L38
	client := otlptracegrpc.NewClient(
		otlptracegrpc.WithEndpoint(config.ENDPOINT),
		otlptracegrpc.WithHeaders(config.HEADERS),
		security(config), // TODO: move it to other place
	)
	exporter, err := otlptrace.New(ctx, client)
	if err != nil {
		return nil, err
	}

	return &exporterWrapperImpl{exporter}, nil
}

func security(config *OTELConfig) otlptracegrpc.Option {
	if config.UseTLS {
		// TODO: check if this implement is correct
		return otlptracegrpc.WithTLSCredentials(credentials.NewClientTLSFromCert(nil, ""))
	}
	return otlptracegrpc.WithInsecure()
}

/*
func initTracer() *trace.TracerProvider {
	// TODO: make it robust
	// see https://github.com/LambdaTest/neuron/blob/9c90d3e427bd059a77d62fff2c0aa960e890e556/pkg/opentelemetry/opentelemetry.go#L24
	// see https://github.com/Imm0bilize/gunshot-core-service/blob/b3d9c0c061ca3de2ad795a3c0b66497f4dfe1139/internal/app/application.go#L31
	// cert on production
	// see https://github.com/kyverno/kyverno/blob/5ec66918f641647d0406b1cc4cb6a2980734cbb1/pkg/tracing/config.go#L36
	// cleanly shutdown
	// see https://github.com/harnitsignalfx/otel-go-kafka-tracing/blob/main/producer/producer.go

	otlptracegrpc.NewClient
	client := otlptracehttp.NewClient()
	exporter, err := otlptrace.New(context.Background(), client)

	if err != nil {
		panic(err)
	}

	// For the demonstration, use sdktrace.AlwaysSample sampler to sample all traces.
	// In a production application, use sdktrace.ProbabilitySampler with a desired probability.
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
		sdktrace.WithBatcher(exporter),
	)
	// registers tp as the global trace provider
	otel.SetTracerProvider(tp)
	// set propagator as the global TextMapPropagator
	otel.SetTextMapPropagator(propagation.TraceContext{})
	return tp
}

*/

// NewOTelTracer - return a new OpenTelemetry tracer wrapper
func NewOTelTracer(config *OTELConfig, options ...func(wrapper *OtelTracerWrapper)) tracer.Tracer {
	w := &OtelTracerWrapper{config: config}
	for _, o := range options {
		o(w)
	}
	w.setDefaults()
	return w
}

func (o *OtelTracerWrapper) setDefaults() {
	if o.exporterWrapperFactory == nil {
		o.exporterWrapperFactory = standardExporter
	}
}

// WithExporterWrapperFactory - override default exporter wrapper factory
func WithExporterWrapperFactory(factory func(ctx context.Context, config *OTELConfig) (ExporterWrapper, error)) func(o *OtelTracerWrapper) {
	// see https://github.com/goadesign/clue/blob/9e6acf66e959879eccdb0b23f032fbed730f15e4/trace/grpc_test.go#L18
	// see https://github.com/rangzen/otel-status/blob/1953278adc88909652d9c25472d7821ab2840396/package/status/http/http_test.go#L49
	// https://github.com/open-feature/go-sdk-contrib/blob/65c70fd7f23104cbb9cd16f49557fc8e705de587/hooks/open-telemetry/pkg/otel_test.go#L27
	return func(o *OtelTracerWrapper) {
		o.exporterWrapperFactory = factory
	}
}

type exporterWrapperImpl struct {
	spanExporter sdktrace.SpanExporter
}

func (e exporterWrapperImpl) RawSpanExporter() sdktrace.SpanExporter {
	return e.spanExporter
}

type ExporterWrapper interface {
	RawSpanExporter() sdktrace.SpanExporter
}

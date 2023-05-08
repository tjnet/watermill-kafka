package otel

type OTELConfig struct {
	// address to send traces to
	ENDPOINT string `env:"OTEL_ADRESS"`
	// if it's true, use tls
	UseTLS bool
	// headers to attach each tracing message
	HEADERS map[string]string
	// service name
	SERVICENAME string
	// version
	VERSION string
}

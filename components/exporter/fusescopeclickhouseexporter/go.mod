module github.com/fuseone-io/fusescope-collector-server/components/exporter/fusescopeclickhouseexporter

go 1.26.0

require (
	github.com/ClickHouse/ch-go v0.75.0
	go.opentelemetry.io/collector/component v1.70.0
	go.opentelemetry.io/collector/component/componenttest v0.164.0
	go.opentelemetry.io/collector/config/configoptional v1.70.0
	go.opentelemetry.io/collector/config/configretry v1.70.0
	go.opentelemetry.io/collector/config/configtls v1.70.0
	go.opentelemetry.io/collector/confmap v1.70.0
	go.opentelemetry.io/collector/consumer v1.70.0
	go.opentelemetry.io/collector/consumer/consumererror v0.164.0
	go.opentelemetry.io/collector/exporter v1.70.0
	go.opentelemetry.io/collector/exporter/exporterhelper v0.164.0
	go.opentelemetry.io/collector/exporter/exportertest v0.164.0
	go.opentelemetry.io/collector/pdata v1.70.0
	go.uber.org/zap v1.28.0
)

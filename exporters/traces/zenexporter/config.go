package zentraceexporter

import (
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/exporter/exporterhelper"
)
type Config struct {
	exporterhelper.Option `mapstructure:"squash"`
	Datasource string `mapstructure:"datasource"`
	Migrations string `mapstructure:"migrations"`
	
}

var _ component.Config = (*Config)(nil)
package myprocessor

import (
	"context"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/processor"
)




const (
	// The value of "type" key in configuration.
	typeStr = "myprocessor"
)
func NewFactory() processor.Factory {
	return processor.NewFactory(
		typeStr,
		createDefaultConfig,
		processor.WithTraces(createTraces, component.StabilityLevelStable),
	)
}

func createDefaultConfig() component.Config {
	return &Config{
	}
}

func createTraces(
	context context.Context,
	set processor.CreateSettings,
	cfg component.Config,
	nextConsumer consumer.Traces,
) (processor.Traces, error) {
	return newBatchTracesProcessor(context, set, nextConsumer, cfg.(*Config))
}

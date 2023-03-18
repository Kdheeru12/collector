package main

import (
	"fmt"
	"log"

	"custom-otel/components"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/otelcol"
)

func main() {

	factories, err := components.Components()
	if err != nil {
		log.Fatalf("failed to build default components: %v", err)
	}

	info := component.BuildInfo{
		Command:     "custom-otel",
		Description: "custom-otel Collector",
		Version:     "latest",
	}

	params := otelcol.CollectorSettings{
		Factories: factories,
		BuildInfo: info,
	}

	if err := runInteractive(params); err != nil {
		log.Fatal(err)
	}
}

func runInteractive(params otelcol.CollectorSettings) error {
	cmd := otelcol.NewCommand(params)
	err := cmd.Execute()
	if err != nil {
		return fmt.Errorf("application run finished with error: %w", err)
	}

	return nil
}

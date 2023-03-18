package myprocessor

import (
	"context"
	"fmt"

	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/collector/processor"
	"go.opentelemetry.io/collector/processor/processorhelper"
)





var processorCapabilities = consumer.Capabilities{MutatesData: true}

func ProcessTracesFunc (ctx context.Context,td ptrace.Traces) (ptrace.Traces, error) {
	// print trace attributes and spans and iterate 
	// over spans and print span attributes
	for i := 0; i < td.ResourceSpans().Len(); i++ {
		rs := td.ResourceSpans().At(i)
		ilss := rs.ScopeSpans()
		fmt.Println("ResourceSpans: ",rs.Resource().Attributes())
		fmt.Println("InstrumentationLibrarySpans: ",ilss)
	}
	return td, nil

}

func newBatchTracesProcessor(ctx context.Context, set processor.CreateSettings, next consumer.Traces, cfg *Config) (processor.Traces,error) {
	fmt.Println(set)
	fmt.Println(next)
	fmt.Println(cfg)
	return processorhelper.NewTracesProcessor(ctx, set, cfg, next,ProcessTracesFunc)
}
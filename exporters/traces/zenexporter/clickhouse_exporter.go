package zentraceexporter

import (
	"context"

	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.uber.org/zap"
)


type storage struct {
	Writer         Writer
	// usageCollector *usage.UsageCollector
	// config         storageConfig
}
func newExporter(cfg *Config, logger *zap.Logger) (*storage, error) {

	newfactory := ClickHouseFactory(cfg.Datasource, cfg.Migrations)
	err := newfactory.Initialize(logger)
	if err != nil {
		return nil, err
	}

	spanWriter, err := newfactory.CreateSpanWriter()
	if err != nil {
		return nil, err
	}
	storage := storage{Writer: spanWriter}
	return &storage, nil
}


// traceDataPusher implements OTEL exporterhelper.traceDataPusher
func (s *storage) pushTraceData(ctx context.Context, td ptrace.Traces) error {

	rss := td.ResourceSpans()
	for i := 0; i < rss.Len(); i++ {
		// fmt.Printf("ResourceSpans #%d\n", i)
		rs := rss.At(i)

		// serviceName := ServiceNameForResource(rs.Resource())

		ilss := rs.ScopeSpans()
		for j := 0; j < ilss.Len(); j++ {
			// fmt.Printf("InstrumentationLibrarySpans #%d\n", j)
			ils := ilss.At(j)

			spans := ils.Spans()

			for k := 0; k < spans.Len(); k++ {
				// span := spans.At(k)
				// traceID := hex.EncodeToString(span.TraceID())
				// structuredSpan := newStructuredSpan(span, serviceName, rs.Resource(), s.config)
				// structuredSpan := "s"
				// err := s.Writer.PushSpanIntoQueue(structuredSpan)
				// if err != nil {
				// 	zap.S().Error("Error in writing spans to clickhouse: ", err)
				// }
			}
		}
	}

	return nil
}
package zentraceexporter

import (
	"go.opencensus.io/stats"
	"go.opencensus.io/stats/view"
	"go.opencensus.io/tag"
)

const (
	SigNozSentSpansKey      = "zen_sent_spans"
	SigNozSentSpansBytesKey = "zen_sent_spans_bytes"
)

var (
	TagTenantKey, _ = tag.NewKey("tenant")
)
var (
	// Measures for usage
	ExporterSigNozSentSpans = stats.Int64(
		SigNozSentSpansKey,
		"Number of signoz log records successfully sent to destination.",
		stats.UnitDimensionless)
	ExporterSigNozSentSpansBytes = stats.Int64(
		SigNozSentSpansBytesKey,
		"Total size of signoz log records successfully sent to destination.",
		stats.UnitDimensionless)

	// Views for usage
	SpansCountView = &view.View{
		Name:        "signoz_spans_count",
		Measure:     ExporterSigNozSentSpans,
		Description: "The number of spans exported to signoz",
		Aggregation: view.Sum(),
		TagKeys:     []tag.Key{TagTenantKey},
	}
	SpansCountBytesView = &view.View{
		Name:        "signoz_spans_bytes",
		Measure:     ExporterSigNozSentSpansBytes,
		Description: "The size of spans exported to signoz",
		Aggregation: view.Sum(),
		TagKeys:     []tag.Key{TagTenantKey},
	}
)

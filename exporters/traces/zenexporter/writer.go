package zentraceexporter

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"strings"
	"sync"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"go.opencensus.io/stats/view"
	"go.uber.org/zap"
)


type SpanWriter struct {
	logger         *zap.Logger
	db             clickhouse.Conn
	traceDatabase  string
	indexTable     string
	errorTable     string
	spansTable     string
	attributeTable string
	encoding       Encoding
	delay          time.Duration
	size           int
	spans          chan *Span
	finish         chan bool
	done           sync.WaitGroup
}

type WriterOptions struct {
	logger         *zap.Logger
	db             clickhouse.Conn
	traceDatabase  string
	spansTable     string
	indexTable     string
	errorTable     string
	attributeTable string
	encoding       Encoding
	delay          time.Duration
	size           int
}

func (w *SpanWriter) PushSpanIntoQueue(span *Span) error {
	w.spans <- span
	return nil
}


func TraceWriter(options WriterOptions) *SpanWriter {
	if err := view.Register(SpansCountView, SpansCountBytesView); err != nil {
		return nil
	}
	writer := &SpanWriter{
		logger:         options.logger,
		db:             options.db,
		traceDatabase:  options.traceDatabase,
		indexTable:     options.indexTable,
		errorTable:     options.errorTable,
		spansTable:     options.spansTable,
		attributeTable: options.attributeTable,
		encoding:       options.encoding,
		delay:          options.delay,
		size:           options.size,
		spans:          make(chan *Span, options.size),
		finish:         make(chan bool),
	}

	go writer.backgroundWriter()

	return writer
}

func (w *SpanWriter) backgroundWriter() {
	batch := make([]*Span, 0, w.size)

	timer := time.After(w.delay)
	last := time.Now()

	for {
		w.done.Add(1)

		flush := false
		finish := false

		select {
		case span := <-w.spans:
			batch = append(batch, span)
			flush = len(batch) == cap(batch)
		case <-timer:
			timer = time.After(w.delay)
			flush = time.Since(last) > w.delay && len(batch) > 0
		case <-w.finish:
			finish = true
			flush = len(batch) > 0
		}

		if flush {
			if err := w.writeBatch(batch); err != nil {
				w.logger.Error("Could not write a batch of spans", zap.Error(err))
			}

			batch = make([]*Span, 0, w.size)
			last = time.Now()
		}

		w.done.Done()

		if finish {
			break
		}
	}
}

func (w *SpanWriter) writeBatch(batch []*Span) error {
	// fmt.Printf("%#v\n", w)	
	// fmt.Println("..........")
	// fmt.Printf("%#v\n", batch)
	// fmt.Print(w.spansTable, w.indexTable,  w.errorTable, w.attributeTable)

	if w.spansTable != "" {
		if err := w.writeSpanData(batch); err != nil {
			logBatch := batch[:int(math.Min(10, float64(len(batch))))]
			w.logger.Error("Could not write a batch of spans to model table: ", zap.Any("batch", logBatch), zap.Error(err))
			return err
		}
	}
	if w.indexTable != "" {
		if err := w.writeTraces(batch); err != nil {
			logBatch := batch[:int(math.Min(10, float64(len(batch))))]
			w.logger.Error("Could not write a batch of spans to index table: ", zap.Any("batch", logBatch), zap.Error(err))
			return err
		}
	}
	if w.errorTable != "" {
		if err := w.writeErrorBatch(batch); err != nil {
			logBatch := batch[:int(math.Min(10, float64(len(batch))))]
			w.logger.Error("Could not write a batch of spans to error table: ", zap.Any("batch", logBatch), zap.Error(err))
			return err
		}
	}
	// if w.attributeTable != "" {
	// 	if err := w.writeTagBatch(batch); err != nil {
	// 		logBatch := batch[:int(math.Min(10, float64(len(batch))))]
	// 		w.logger.Error("Could not write a batch of spans to tag table: ", zap.Any("batch", logBatch), zap.Error(err))
	// 		return err
	// 	}
	// }

	return nil
}


func (w *SpanWriter) writeSpanData(batchSpans []*Span) error {
	// fmt.Println("inside write spandata")
	fmt.Printf("INSERT INTO %s.%s", w.traceDatabase, w.spansTable)
	ctx := context.Background()
	statement, err := w.db.PrepareBatch(ctx, fmt.Sprintf("INSERT INTO %s.%s", w.traceDatabase, w.spansTable))
	if err != nil {
		logBatch := batchSpans[:int(math.Min(10, float64(len(batchSpans))))]
		w.logger.Error("Could not prepare batch for model table: ", zap.Any("batch", logBatch), zap.Error(err))
		return err
	}

	// metrics := map[string]usage.Metric{}
	for _, span := range batchSpans {
		var serialized []byte

		serialized, err = json.Marshal(span.TraceModel)

		if err != nil {
			return err
		}
		// fmt.Println(string(serialized))
		err = statement.Append(time.Unix(0, int64(span.StartTimeUnixNano)), span.TraceId, string(serialized))
		if err != nil {
			w.logger.Error("Could not append span to batch: ", zap.Object("span", span), zap.Error(err))
			return err
		}

		// usage.AddMetric(metrics, *span.Tenant, 1, int64(len(serialized)))
	}
	// start := time.Now()
	fmt.Println(statement)

	err = statement.Send()
	// ctx, _ = tag.New(ctx,
	// 	tag.Upsert(exporterKey, string(component.DataTypeTraces)),
	// 	tag.Upsert(tableKey, w.spansTable),
	// )
	// stats.Record(ctx, writeLatencyMillis.M(int64(time.Since(start).Milliseconds())))
	if err != nil {
		fmt.Print("error is saVIG ERR")
		return err
	}
	// for k, v := range metrics {
	// 	stats.RecordWithTags(ctx, []tag.Mutator{tag.Upsert(usage.TagTenantKey, k)}, ExporterSigNozSentSpans.M(int64(v.Count)), ExporterSigNozSentSpansBytes.M(int64(v.Size)))
	// }

	return nil
}



func (w *SpanWriter) writeTraces(batchSpans []*Span) error {

	ctx := context.Background()
	statement, err := w.db.PrepareBatch(ctx, fmt.Sprintf("INSERT INTO %s.%s", w.traceDatabase, w.indexTable))
	if err != nil {
		logBatch := batchSpans[:int(math.Min(10, float64(len(batchSpans))))]
		w.logger.Error("Could not prepare batch for index table: ", zap.Any("batch", logBatch), zap.Error(err))
		return err
	}

	for _, span := range batchSpans {
		err = statement.Append(
			time.Unix(0, int64(span.StartTimeUnixNano)),
			span.TraceId,
			span.SpanId,
			span.ParentSpanId,
			span.ServiceName,
			span.Name,
			span.Kind,
			span.DurationNano,
			span.StatusCode,
			span.ExternalHttpMethod,
			span.ExternalHttpUrl,
			span.Component,
			span.DBSystem,
			span.DBName,
			span.DBOperation,
			span.PeerService,
			span.Events,
			span.HttpMethod,
			span.HttpUrl,
			span.HttpCode,
			span.HttpRoute,
			span.HttpHost,
			span.MsgSystem,
			span.MsgOperation,
			span.HasError,
			span.TagMap,
			span.GRPCMethod,
			span.GRPCCode,
			span.RPCSystem,
			span.RPCService,
			span.RPCMethod,
			span.ResponseStatusCode,
			span.StringTagMap,
			span.NumberTagMap,
			span.BoolTagMap,
			span.ResourceTagsMap,
		)
		if err != nil {
			w.logger.Error("Could not append span to batch: ", zap.Object("span", span), zap.Error(err))
			return err
		}
	}

	// start := time.Now()

	err = statement.Send()

	// ctx, _ = tag.New(ctx,
	// 	tag.Upsert(exporterKey, string(component.DataTypeTraces)),
	// 	tag.Upsert(tableKey, w.indexTable),
	// )
	// stats.Record(ctx, writeLatencyMillis.M(int64(time.Since(start).Milliseconds())))
	return err
}


func (w *SpanWriter) writeErrorBatch(batchSpans []*Span) error {

	ctx := context.Background()
	statement, err := w.db.PrepareBatch(ctx, fmt.Sprintf("INSERT INTO %s.%s", w.traceDatabase, w.errorTable))
	if err != nil {
		logBatch := batchSpans[:int(math.Min(10, float64(len(batchSpans))))]
		w.logger.Error("Could not prepare batch for error table: ", zap.Any("batch", logBatch), zap.Error(err))
		return err
	}

	for _, span := range batchSpans {
		if span.ErrorEvent.Name == "" {
			continue
		}
		err = statement.Append(
			time.Unix(0, int64(span.ErrorEvent.TimeUnixNano)),
			span.ErrorID,
			span.ErrorGroupID,
			span.TraceId,
			span.SpanId,
			span.ServiceName,
			span.ErrorEvent.AttributeMap["exception.type"],
			span.ErrorEvent.AttributeMap["exception.message"],
			span.ErrorEvent.AttributeMap["exception.stacktrace"],
			stringToBool(span.ErrorEvent.AttributeMap["exception.escaped"]),
			span.ResourceTagsMap,
		)
		if err != nil {
			w.logger.Error("Could not append span to batch: ", zap.Object("span", span), zap.Error(err))
			return err
		}
	}

	// start := time.Now()

	err = statement.Send()

	// ctx, _ = tag.New(ctx,
	// 	tag.Upsert(exporterKey, string(component.DataTypeTraces)),
	// 	tag.Upsert(tableKey, w.errorTable),
	// )
	// stats.Record(ctx, writeLatencyMillis.M(int64(time.Since(start).Milliseconds())))
	return err
}

func stringToBool(s string) bool {
	return strings.ToLower(s) == "true"
}
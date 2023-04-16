package zentraceexporter

import (
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

	// if w.spansTable != "" {
	// 	if err := w.writeModelBatch(batch); err != nil {
	// 		logBatch := batch[:int(math.Min(10, float64(len(batch))))]
	// 		w.logger.Error("Could not write a batch of spans to model table: ", zap.Any("batch", logBatch), zap.Error(err))
	// 		return err
	// 	}
	// }
	// if w.indexTable != "" {
	// 	if err := w.writeIndexBatch(batch); err != nil {
	// 		logBatch := batch[:int(math.Min(10, float64(len(batch))))]
	// 		w.logger.Error("Could not write a batch of spans to index table: ", zap.Any("batch", logBatch), zap.Error(err))
	// 		return err
	// 	}
	// }
	// if w.errorTable != "" {
	// 	if err := w.writeErrorBatch(batch); err != nil {
	// 		logBatch := batch[:int(math.Min(10, float64(len(batch))))]
	// 		w.logger.Error("Could not write a batch of spans to error table: ", zap.Any("batch", logBatch), zap.Error(err))
	// 		return err
	// 	}
	// }
	// if w.attributeTable != "" {
	// 	if err := w.writeTagBatch(batch); err != nil {
	// 		logBatch := batch[:int(math.Min(10, float64(len(batch))))]
	// 		w.logger.Error("Could not write a batch of spans to tag table: ", zap.Any("batch", logBatch), zap.Error(err))
	// 		return err
	// 	}
	// }

	return nil
}

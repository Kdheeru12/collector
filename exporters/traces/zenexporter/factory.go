package zentraceexporter

import (
	"context"
	"fmt"
	"net/url"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/golang-migrate/migrate/v4"
	"go.opencensus.io/stats"
	"go.opencensus.io/stats/view"
	"go.opencensus.io/tag"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/exporter/exporterhelper"
	"go.uber.org/zap"
)

const (
	// The value of "type" key in configuration.
	typeStr = "zentraceexporter"
	primaryNamespace = "clickhouse"
	archiveNamespace = "clickhouse-archive"
)

type Writer interface { // Writer is an interface that allows writing spans to push data into queue
	PushSpanIntoQueue(span *Span) error
}

type writerMaker func(WriterOptions) (Writer, error)


type Factory struct {
	logger     *zap.Logger
	Settings    *ClickHouseSettings
	dbClient   clickhouse.Conn
	archive    clickhouse.Conn
	datasource string
	makeWriter writerMaker
}



func createDefaultConfig() component.Config {
	return &Config{
		
	}
}
// NewFactory creates a factory for the zentraceexporter.
func NewFactory() exporter.Factory {

	return exporter.NewFactory(
		typeStr,
		createDefaultConfig,
		exporter.WithTraces(createTracesExporter, component.StabilityLevelDevelopment),
	)
}

var (
	writeLatencyMillis = stats.Int64("exporter_db_write_latency", "Time taken (in millis) for exporter to write batch", "ms")
	exporterKey        = tag.MustNewKey("exporter")
	tableKey           = tag.MustNewKey("table")
)

func createTracesExporter(ctx context.Context, settings exporter.CreateSettings, config component.Config) (exporter.Traces, error) {
	cfg := config.(*Config)
	tetr, error := newExporter(cfg, settings.TelemetrySettings.Logger)
	if error != nil {
		return nil, error
	}

	// exporterLogger := createLogger(cfg, set.TelemetrySettings.Logger)
	// s := newLoggingExporter(exporterLogger, cfg.Verbosity)
	return exporterhelper.NewTracesExporter(
		ctx,
		settings,
		cfg,
		tetr.pushTraceData,
		exporterhelper.WithCapabilities(consumer.Capabilities{MutatesData: false}),
		// Disable Timeout/RetryOnFailure and SendingQueue
		exporterhelper.WithTimeout(exporterhelper.TimeoutSettings{Timeout: 0}),
		exporterhelper.WithRetry(exporterhelper.RetrySettings{Enabled: false}),
		exporterhelper.WithQueue(exporterhelper.QueueSettings{Enabled: false}),
	)
}



func ClickHouseFactory(migrations string, datasource string) *Factory {
	writeLatencyDistribution := view.Distribution(100, 250, 500, 750, 1000, 2000, 4000, 8000, 16000, 32000, 64000, 128000, 256000, 512000)

	writeLatencyView := &view.View{
		Name:        "exporter_db_write_latency",
		Measure:     writeLatencyMillis,
		Description: writeLatencyMillis.Description(),
		TagKeys:     []tag.Key{exporterKey, tableKey},
		Aggregation: writeLatencyDistribution,
	}

	view.Register(writeLatencyView)
	
	return &Factory{
		Settings: GetDefaultSettings(migrations, datasource, primaryNamespace, archiveNamespace),
		// makeReader: func(db *clickhouse.Conn, operationsTable, indexTable, spansTable string) (spanstore.Reader, error) {
		// 	return store.NewTraceReader(db, operationsTable, indexTable, spansTable), nil
		// },
		makeWriter: func(options WriterOptions) (Writer, error) {
			return TraceWriter(options), nil
		},
	}
}

func (f *Factory) Initialize(logger *zap.Logger) error {
	f.logger = logger

	dbClient, err := f.connect(f.Settings.defaultConfig)
	
	if err != nil {
		return fmt.Errorf("error connecting to primary db: %v", err)
	}
	f.dbClient = dbClient

	archiveConfig := f.Settings.others[archiveNamespace]
	if archiveConfig.Enabled {
		archive, err := f.connect(archiveConfig)
		if err != nil {
			return fmt.Errorf("error connecting to archive db: %v", err)
		}

		f.archive = archive
	}
	f.logger.Info("Running migrations from path: ", zap.Any("test", f.Settings.defaultConfig.Migrations))

	clickhouseUrl, err := buildClickhouseMigrateURL(f.Settings.defaultConfig.Datasource, f.Settings.defaultConfig.Cluster)
	if err != nil {
		return fmt.Errorf("error building clickhouse migrate url: %v", err)
	}

	m, err := migrate.New(
		"file://"+f.Settings.defaultConfig.Migrations,
		clickhouseUrl)
	if err != nil {
		return fmt.Errorf("Clickhouse Migrate failed to run, error: %s", err)
	}
	m.Up()
	f.logger.Info("Clickhouse Migrate finished", zap.Error(err))
	return nil
}


func (f *Factory) connect(cfg *clickHouseConfig) (clickhouse.Conn, error) {
	if cfg.Encoding != EncodingJSON && cfg.Encoding != EncodingProto {
		return nil, fmt.Errorf("unknown encoding %q, supported: %q, %q", cfg.Encoding, EncodingJSON, EncodingProto)
	}

	return cfg.DBConnector(cfg)
}

func buildClickhouseMigrateURL(datasource string, cluster string) (string, error) {
	// return fmt.Sprintf("clickhouse://localhost:9000?database=default&x-multi-statement=true"), nil
	var clickhouseUrl string
	database := "zen_traces"
	parsedURL, err := url.Parse(datasource)
	if err != nil {
		return "", err
	}
	host := parsedURL.Host
	if host == "" {
		return "", fmt.Errorf("unable to parse host")

	}
	paramMap, err := url.ParseQuery(parsedURL.RawQuery)
	if err != nil {
		return "", err
	}
	username := paramMap["username"]
	password := paramMap["password"]

	if len(username) > 0 && len(password) > 0 {
		clickhouseUrl = fmt.Sprintf("clickhouse://%s:%s@%s/%s?x-multi-statement=true&x-cluster-name=%s&x-migrations-table=schema_migrations&x-migrations-table-engine=MergeTree", username[0], password[0], host, database, cluster)
	} else {
		clickhouseUrl = fmt.Sprintf("clickhouse://%s/%s?x-multi-statement=true&x-cluster-name=%s&x-migrations-table=schema_migrations&x-migrations-table-engine=MergeTree", host, database, cluster)
	}
	return clickhouseUrl, nil
}


func (f *Factory) CreateSpanWriter() (Writer, error) {
	cfg := f.Settings.defaultConfig
	return f.makeWriter(WriterOptions{
		logger:         f.logger,
		db:             f.dbClient,
		traceDatabase:  cfg.TraceDatabase,
		spansTable:     cfg.SpansTable,
		indexTable:     cfg.IndexTable,
		errorTable:     cfg.ErrorTable,
		attributeTable: cfg.AttributeTable,
		encoding:       cfg.Encoding,
		delay:          cfg.WriteBatchDelay,
		size:           cfg.WriteBatchSize,
	})
}
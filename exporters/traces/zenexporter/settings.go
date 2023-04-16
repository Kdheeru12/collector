package zentraceexporter

import (
	"context"
	"fmt"
	"net/url"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
)

type Encoding string

const (
	// EncodingJSON is used for spans encoded as JSON.
	EncodingJSON Encoding = "json"
	// EncodingProto is used for spans encoded as Protobuf.
	EncodingProto Encoding = "protobuf"
)


const (
	defaultDatasource               string        = "tcp://127.0.0.1:9000/?database=signoz_traces"
	defaultTraceDatabase            string        = "signoz_traces"
	defaultMigrations               string        = "/migrations"
	defaultOperationsTable          string        = "distributed_signoz_operations"
	defaultIndexTable               string        = "distributed_signoz_index_v2"
	localIndexTable                 string        = "signoz_index_v2"
	defaultErrorTable               string        = "distributed_signoz_error_index_v2"
	defaultSpansTable               string        = "distributed_signoz_spans"
	defaultAttributeTable           string        = "distributed_span_attributes"
	defaultDurationSortTable        string        = "durationSort"
	defaultDurationSortMVTable      string        = "durationSortMV"
	defaultArchiveSpansTable        string        = "signoz_archive_spans"
	defaultClusterName              string        = "cluster"
	defaultDependencyGraphTable     string        = "dependency_graph_minutes"
	defaultDependencyGraphServiceMV string        = "dependency_graph_minutes_service_calls_mv"
	defaultDependencyGraphDbMV      string        = "dependency_graph_minutes_db_calls_mv"
	DependencyGraphMessagingMV      string        = "dependency_graph_minutes_messaging_calls_mv"
	defaultWriteBatchDelay          time.Duration = 2 * time.Second
	defaultWriteBatchSize           int           = 100000
	defaultEncoding                 Encoding      = EncodingJSON
)

type clickHouseConfig struct {
	namespace                  string
	Enabled                    bool
	Datasource                 string
	Migrations                 string
	TraceDatabase              string
	OperationsTable            string
	IndexTable                 string
	LocalIndexTable            string
	SpansTable                 string
	ErrorTable                 string
	AttributeTable             string
	Cluster                    string
	DurationSortTable          string
	DurationSortMVTable        string
	DependencyGraphServiceMV   string
	DependencyGraphDbMV        string
	DependencyGraphMessagingMV string
	DependencyGraphTable       string
	// DockerMultiNodeCluster     bool
	WriteBatchDelay            time.Duration
	WriteBatchSize             int
	Encoding                   Encoding
	DBConnector                DatabaseConnector
}

type DatabaseConnector func(cfg *clickHouseConfig) (clickhouse.Conn, error)


type ClickHouseSettings struct {
	defaultConfig *clickHouseConfig

	others map[string]*clickHouseConfig
}

func GetDefaultSettings(migrations string, datasource string, primaryNamespace string, otherNamespaces ...string) *ClickHouseSettings {
	if datasource == "" {
		datasource = defaultDatasource
	}
	if migrations == "" {
		migrations = defaultMigrations
	}

	options := &ClickHouseSettings{
		defaultConfig: &clickHouseConfig{
			namespace:                  primaryNamespace,
			Enabled:                    true,
			Datasource:                 datasource,
			Migrations:                 migrations,
			TraceDatabase:              defaultTraceDatabase,
			OperationsTable:            defaultOperationsTable,
			IndexTable:                 defaultIndexTable,
			LocalIndexTable:            localIndexTable,
			ErrorTable:                 defaultErrorTable,
			SpansTable:                 defaultSpansTable,
			AttributeTable:             defaultAttributeTable,
			DurationSortTable:          defaultDurationSortTable,
			DurationSortMVTable:        defaultDurationSortMVTable,
			Cluster:                    defaultClusterName,
			DependencyGraphTable:       defaultDependencyGraphTable,
			DependencyGraphServiceMV:   defaultDependencyGraphServiceMV,
			DependencyGraphDbMV:        defaultDependencyGraphDbMV,
			DependencyGraphMessagingMV: DependencyGraphMessagingMV,
			// DockerMultiNodeCluster:     dockerMultiNodeCluster,
			WriteBatchDelay:            defaultWriteBatchDelay,
			WriteBatchSize:             defaultWriteBatchSize,
			Encoding:                   defaultEncoding,
			DBConnector:                  defaultClickHouseConnector,
		},
		others: make(map[string]*clickHouseConfig, len(otherNamespaces)),
	}

	for _, namespace := range otherNamespaces {
		if namespace == archiveNamespace {
			options.others[namespace] = &clickHouseConfig{
				namespace:       namespace,
				Datasource:      datasource,
				Migrations:      migrations,
				OperationsTable: "",
				IndexTable:      "",
				SpansTable:      defaultArchiveSpansTable,
				WriteBatchDelay: defaultWriteBatchDelay,
				WriteBatchSize:  defaultWriteBatchSize,
				Encoding:        defaultEncoding,
				DBConnector:       defaultClickHouseConnector,
			}
		} else {
			options.others[namespace] = &clickHouseConfig{namespace: namespace}
		}
	}

	return options
}


func defaultClickHouseConnector(cfg *clickHouseConfig) (clickhouse.Conn, error) {

	ctx := context.Background()
	url, err := url.Parse(cfg.Datasource)
	options := &clickhouse.Options{
		Addr: []string{url.Host},
	}

	// parse username and password
	if url.Query().Get("username") != "" {
		auth := clickhouse.Auth{
			Username: url.Query().Get("username"),
			Password: url.Query().Get("password"),
		}
		options.Auth = auth
	}

	// open connection
	dbClient, err := clickhouse.Open(options)
	
	if err != nil {
		return nil, err
	}

	if err := dbClient.Ping(ctx); err != nil {
		return nil, err
	}

	// check if database exists or create it	
	query := fmt.Sprintf(`CREATE DATABASE IF NOT EXISTS %s ON CLUSTER %s`, url.Query().Get("database"), cfg.Cluster)

	if err := dbClient.Exec(ctx, query); err != nil {
		return nil, err
	}

	return dbClient, nil

}

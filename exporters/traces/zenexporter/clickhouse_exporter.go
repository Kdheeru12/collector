package zentraceexporter

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/url"
	"strconv"
	"strings"

	"github.com/google/uuid"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.uber.org/zap"
)

type storage struct {
	Writer Writer
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

func getServiceName(res pcommon.Resource) string {
	serviceAttr, found := res.Attributes().Get("service.name")

	if found {
		return serviceAttr.Str()
	}

	return "unknown-service"
}

func TraceIDToHexOrEmptyString(traceID pcommon.TraceID) string {
	if !traceID.IsEmpty() {
		return hex.EncodeToString(traceID[:])
	}
	return ""
}

func SpanIDToHexOrEmptyString(spanID pcommon.SpanID) string {
	if !spanID.IsEmpty() {
		return hex.EncodeToString(spanID[:])
	}
	return ""
}

func addTraceFieldsIntoSpanAttributes(span *Span) []SpanAttribute {
	// value is not used anyways it is redundant utill then
	//jus
	spanAttributes := []SpanAttribute{}
	spanAttributes = append(spanAttributes, SpanAttribute{
		Key:         "traceID",
		TagType:     "tag",
		IsColumn:    true,
		DataType:    "string",
		StringValue: span.TraceId,
	})
	spanAttributes = append(spanAttributes, SpanAttribute{
		Key:         "spanID",
		TagType:     "tag",
		IsColumn:    true,
		DataType:    "string",
		StringValue: span.SpanId,
	})
	spanAttributes = append(spanAttributes, SpanAttribute{
		Key:         "parentSpanID",
		TagType:     "tag",
		IsColumn:    true,
		DataType:    "string",
		StringValue: span.ParentSpanId,
	})
	spanAttributes = append(spanAttributes, SpanAttribute{
		Key:         "name",
		TagType:     "tag",
		IsColumn:    true,
		DataType:    "string",
		StringValue: span.Name,
	})
	spanAttributes = append(spanAttributes, SpanAttribute{
		Key:         "serviceName",
		TagType:     "tag",
		IsColumn:    true,
		DataType:    "string",
		StringValue: span.ServiceName,
	})
	spanAttributes = append(spanAttributes, SpanAttribute{
		Key:         "kind",
		TagType:     "tag",
		IsColumn:    true,
		DataType:    "float64",
		NumberValue: float64(span.Kind),
	})
	spanAttributes = append(spanAttributes, SpanAttribute{
		Key:         "durationNano",
		TagType:     "tag",
		IsColumn:    true,
		DataType:    "float64",
		NumberValue: float64(span.DurationNano),
	})
	spanAttributes = append(spanAttributes, SpanAttribute{
		Key:         "statusCode",
		TagType:     "tag",
		IsColumn:    true,
		DataType:    "float64",
		NumberValue: float64(span.StatusCode),
	})
	spanAttributes = append(spanAttributes, SpanAttribute{
		Key:      "hasError",
		TagType:  "tag",
		IsColumn: true,
		DataType: "bool",
	})
	spanAttributes = append(spanAttributes, SpanAttribute{
		Key:         "externalHttpMethod",
		TagType:     "tag",
		IsColumn:    true,
		DataType:    "string",
		StringValue: span.ExternalHttpMethod,
	})
	spanAttributes = append(spanAttributes, SpanAttribute{
		Key:         "externalHttpUrl",
		TagType:     "tag",
		IsColumn:    true,
		DataType:    "string",
		StringValue: span.ExternalHttpUrl,
	})
	spanAttributes = append(spanAttributes, SpanAttribute{
		Key:         "component",
		TagType:     "tag",
		IsColumn:    true,
		DataType:    "string",
		StringValue: span.Component,
	})
	spanAttributes = append(spanAttributes, SpanAttribute{
		Key:         "dbSystem",
		TagType:     "tag",
		IsColumn:    true,
		DataType:    "string",
		StringValue: span.DBSystem,
	})
	spanAttributes = append(spanAttributes, SpanAttribute{
		Key:         "dbName",
		TagType:     "tag",
		IsColumn:    true,
		DataType:    "string",
		StringValue: span.DBName,
	})
	spanAttributes = append(spanAttributes, SpanAttribute{
		Key:         "dbOperation",
		TagType:     "tag",
		IsColumn:    true,
		DataType:    "string",
		StringValue: span.DBOperation,
	})
	spanAttributes = append(spanAttributes, SpanAttribute{
		Key:         "peerService",
		TagType:     "tag",
		IsColumn:    true,
		DataType:    "string",
		StringValue: span.PeerService,
	})
	spanAttributes = append(spanAttributes, SpanAttribute{
		Key:         "httpMethod",
		TagType:     "tag",
		IsColumn:    true,
		DataType:    "string",
		StringValue: span.HttpMethod,
	})
	spanAttributes = append(spanAttributes, SpanAttribute{
		Key:         "httpUrl",
		TagType:     "tag",
		IsColumn:    true,
		DataType:    "string",
		StringValue: span.HttpUrl,
	})
	spanAttributes = append(spanAttributes, SpanAttribute{
		Key:         "httpRoute",
		TagType:     "tag",
		IsColumn:    true,
		DataType:    "string",
		StringValue: span.HttpRoute,
	})
	spanAttributes = append(spanAttributes, SpanAttribute{
		Key:         "httpHost",
		TagType:     "tag",
		IsColumn:    true,
		DataType:    "string",
		StringValue: span.HttpHost,
	})
	spanAttributes = append(spanAttributes, SpanAttribute{
		Key:         "msgSystem",
		TagType:     "tag",
		IsColumn:    true,
		DataType:    "string",
		StringValue: span.MsgSystem,
	})
	spanAttributes = append(spanAttributes, SpanAttribute{
		Key:         "msgOperation",
		TagType:     "tag",
		IsColumn:    true,
		DataType:    "string",
		StringValue: span.MsgOperation,
	})
	spanAttributes = append(spanAttributes, SpanAttribute{
		Key:         "rpcSystem",
		TagType:     "tag",
		IsColumn:    true,
		DataType:    "string",
		StringValue: span.RPCSystem,
	})
	spanAttributes = append(spanAttributes, SpanAttribute{
		Key:         "rpcService",
		TagType:     "tag",
		IsColumn:    true,
		DataType:    "string",
		StringValue: span.RPCService,
	})
	spanAttributes = append(spanAttributes, SpanAttribute{
		Key:         "rpcMethod",
		TagType:     "tag",
		IsColumn:    true,
		DataType:    "string",
		StringValue: span.RPCMethod,
	})
	spanAttributes = append(spanAttributes, SpanAttribute{
		Key:         "responseStatusCode",
		TagType:     "tag",
		IsColumn:    true,
		DataType:    "string",
		StringValue: span.ResponseStatusCode,
	})
	return spanAttributes
}

func populateEvents(events ptrace.SpanEventSlice, span *Span) {
	for i := 0; i < events.Len(); i++ {
		event := Event{}
		event.Name = events.At(i).Name()
		event.TimeUnixNano = uint64(events.At(i).Timestamp())
		event.AttributeMap = map[string]string{}
		event.IsError = false
		events.At(i).Attributes().Range(func(k string, v pcommon.Value) bool {
			event.AttributeMap[k] = v.AsString()
			return true
		})
		if event.Name == "exception" {
			event.IsError = true
			span.ErrorEvent = event
			uuidWithHyphen := uuid.New()
			uuid := strings.Replace(uuidWithHyphen.String(), "-", "", -1)
			span.ErrorID = uuid
			var hash [16]byte
			hash = md5.Sum([]byte(span.ServiceName + span.ErrorEvent.AttributeMap["exception.type"]))
			// if lowCardinalExceptionGrouping {
			// 	hash = md5.Sum([]byte(span.ServiceName + span.ErrorEvent.AttributeMap["exception.type"]))
			// } else {
			// 	hash = md5.Sum([]byte(span.ServiceName + span.ErrorEvent.AttributeMap["exception.type"] + span.ErrorEvent.AttributeMap["exception.message"]))

			// }
			span.ErrorGroupID = fmt.Sprintf("%x", hash)
		}
		stringEvent, _ := json.Marshal(event)
		span.Events = append(span.Events, string(stringEvent))
	}
}

func createStructuredSpan(receivedspan ptrace.Span, ServiceName string, resource pcommon.Resource) *Span {
	spanDurationNano := uint64(receivedspan.EndTimestamp() - receivedspan.StartTimestamp())
	tagMap := map[string]string{}
	stringTagMap := map[string]string{}
	numberTagMap := map[string]float64{}
	boolTagMap := map[string]bool{}
	spanAttributes := []SpanAttribute{}
	attributes := receivedspan.Attributes()
	resourceAttrs := map[string]string{}
	resourceAttributes := resource.Attributes()

	attributes.Range(func(k string, v pcommon.Value) bool {
		tagMap[k] = v.AsString()
		spanAttribute := SpanAttribute{
			Key:      k,
			TagType:  "tag",
			IsColumn: false,
		}
		if v.Type() == pcommon.ValueTypeDouble {
			numberTagMap[k] = v.Double()
			spanAttribute.NumberValue = v.Double()
			spanAttribute.DataType = "float64"
		} else if v.Type() == pcommon.ValueTypeInt {
			numberTagMap[k] = float64(v.Int())
			spanAttribute.NumberValue = float64(v.Int())
			spanAttribute.DataType = "float64"
		} else if v.Type() == pcommon.ValueTypeBool {
			boolTagMap[k] = v.Bool()
			spanAttribute.DataType = "bool"
		} else {
			stringTagMap[k] = v.AsString()
			spanAttribute.StringValue = v.AsString()
			spanAttribute.DataType = "string"
		}
		spanAttributes = append(spanAttributes, spanAttribute)
		return true

	})

	resourceAttributes.Range(func(k string, v pcommon.Value) bool {
		tagMap[k] = v.AsString()
		spanAttribute := SpanAttribute{
			Key:      k,
			TagType:  "resource",
			IsColumn: false,
		}
		resourceAttrs[k] = v.AsString()
		if v.Type() == pcommon.ValueTypeDouble {
			numberTagMap[k] = v.Double()
			spanAttribute.NumberValue = v.Double()
			spanAttribute.DataType = "float64"
		} else if v.Type() == pcommon.ValueTypeInt {
			numberTagMap[k] = float64(v.Int())
			spanAttribute.NumberValue = float64(v.Int())
			spanAttribute.DataType = "float64"
		} else if v.Type() == pcommon.ValueTypeBool {
			boolTagMap[k] = v.Bool()
			spanAttribute.DataType = "bool"
		} else {
			stringTagMap[k] = v.AsString()
			spanAttribute.StringValue = v.AsString()
			spanAttribute.DataType = "string"
		}
		spanAttributes = append(spanAttributes, spanAttribute)
		return true

	})

	var span *Span = &Span{
		ServiceName:       ServiceName,
		TraceId:           TraceIDToHexOrEmptyString(receivedspan.TraceID()),
		ParentSpanId:      SpanIDToHexOrEmptyString(receivedspan.ParentSpanID()),
		SpanId:            SpanIDToHexOrEmptyString(receivedspan.SpanID()),
		Name:              receivedspan.Name(),
		Kind:              int8(receivedspan.Kind()),
		StartTimeUnixNano: uint64(receivedspan.StartTimestamp()),
		EndTimeUnixNano:   uint64(receivedspan.EndTimestamp()),
		DurationNano:      spanDurationNano,
		StatusCode:        int16(receivedspan.Status().Code()),
		TagMap:            tagMap,
		StringTagMap:      stringTagMap,
		NumberTagMap:      numberTagMap,
		BoolTagMap:        boolTagMap,
		ResourceTagsMap:   resourceAttrs,
		HasError:          int16(receivedspan.Status().Code()) == 2,
		TraceModel: TraceModel{
			TraceId:           TraceIDToHexOrEmptyString(receivedspan.TraceID()),
			SpanId:            SpanIDToHexOrEmptyString(receivedspan.SpanID()),
			Name:              receivedspan.Name(),
			DurationNano:      spanDurationNano,
			StartTimeUnixNano: uint64(receivedspan.StartTimestamp()),
			ServiceName:       ServiceName,
			Kind:              int8(receivedspan.Kind()),
			TagMap:            tagMap,
			StringTagMap:      stringTagMap,
			NumberTagMap:      numberTagMap,
			BoolTagMap:        boolTagMap,
			HasError:          int16(receivedspan.Status().Code()) == 2,
		},
	}

	attributes.Range(func(k string, v pcommon.Value) bool {
		if k == "http.status_code" {
			if v.Int() >= 400 {
				span.HasError = true
			}
			span.HttpCode = strconv.FormatInt(v.Int(), 10)
			span.ResponseStatusCode = span.HttpCode
		} else if k == "http.url" && span.Kind == 3 {
			value := v.Str()
			valueUrl, err := url.Parse(value)
			if err == nil {
				value = valueUrl.Hostname()
			}
			span.ExternalHttpUrl = value
			span.HttpUrl = v.Str()
		} else if k == "http.method" && span.Kind == 3 {
			span.ExternalHttpMethod = v.Str()
			span.HttpMethod = v.Str()
		} else if k == "http.url" && span.Kind != 3 {
			span.HttpUrl = v.Str()
		} else if k == "http.method" && span.Kind != 3 {
			span.HttpMethod = v.Str()
		} else if k == "http.route" {
			span.HttpRoute = v.Str()
		} else if k == "http.host" {
			span.HttpHost = v.Str()
		} else if k == "messaging.system" {
			span.MsgSystem = v.Str()
		} else if k == "messaging.operation" {
			span.MsgOperation = v.Str()
		} else if k == "component" {
			span.Component = v.Str()
		} else if k == "db.system" {
			span.DBSystem = v.Str()
		} else if k == "db.name" {
			span.DBName = v.Str()
		} else if k == "db.operation" {
			span.DBOperation = v.Str()
		} else if k == "peer.service" {
			span.PeerService = v.Str()
		} else if k == "rpc.grpc.status_code" {
			// Handle both string/int status code in GRPC spans.
			statusString, err := strconv.Atoi(v.Str())
			statusInt := v.Int()
			if err == nil && statusString != 0 {
				statusInt = int64(statusString)
			}
			if statusInt >= 2 {
				span.HasError = true
			}
			span.GRPCCode = strconv.FormatInt(statusInt, 10)
			span.ResponseStatusCode = span.GRPCCode
		} else if k == "rpc.method" {
			span.RPCMethod = v.Str()
			system, found := attributes.Get("rpc.system")
			if found && system.Str() == "grpc" {
				span.GRPCMethod = v.Str()
			}
		} else if k == "rpc.service" {
			span.RPCService = v.Str()
		} else if k == "rpc.system" {
			span.RPCSystem = v.Str()
		} else if k == "rpc.jsonrpc.error_code" {
			span.ResponseStatusCode = v.Str()
		}
		return true
	})

	populateEvents(receivedspan.Events(), span)
	span.TraceModel.Events = span.Events
	span.TraceModel.HasError = span.HasError
	spanAttributes = append(spanAttributes, addTraceFieldsIntoSpanAttributes(span)...)
	span.SpanAttributes = spanAttributes

	return span

}

// traceDataPusher implements OTEL exporterhelper.traceDataPusher
func (s *storage) pushTraceData(ctx context.Context, td ptrace.Traces) error {

	rss := td.ResourceSpans()
	for i := 0; i < rss.Len(); i++ {
		// fmt.Printf("ResourceSpans #%d\n", i)
		rs := rss.At(i)

		serviceName := getServiceName(rs.Resource())

		// InstrumentationLibrarySpans
		ilss := rs.ScopeSpans()
		for j := 0; j < ilss.Len(); j++ {
			// fmt.Printf("InstrumentationLibrarySpans #%d\n", j)
			ils := ilss.At(j)

			spans := ils.Spans()

			for k := 0; k < spans.Len(); k++ {
				span := spans.At(k)
				// traceID := hex.EncodeToString(span.TraceID())
				structuredSpan := createStructuredSpan(span, serviceName, rs.Resource())
				// structuredSpan := "s"
				// var data []byte
				// encoder := zapcore.NewJSONEncoder(zapcore.EncoderConfig{})
				err := s.Writer.PushSpanIntoQueue(structuredSpan)
				// fmt.Println(structuredSpan, json.Unmarshal(data, &structuredSpan), structuredSpan.MarshalLogObject(encoder), "dakmladl")
				// fmt.Printf("%+v\n", structuredSpan)
				// fmt.Printf("%#v\n", structuredSpan)
				if err != nil {
					zap.S().Error("Error in writing spans to clickhouse: ", err)
				}
			}
		}
	}

	return nil
}

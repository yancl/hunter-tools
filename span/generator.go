package span

import (
	"fmt"
	"github.com/census-instrumentation/opencensus-proto/gen-go/traceproto"
	proto "github.com/golang/protobuf/proto"
	timestamp "github.com/golang/protobuf/ptypes/timestamp"
	"github.com/yancl/hunter-proto/gen-go/dumpproto"
	hutils "github.com/yancl/hunter-tools/utils"
	"time"
)

const (
	SERVICE_NAME = "service_name"
	REMOTE_KIND  = "remote_kind"
	QUERY        = "query"
)

const (
	REMOTE_KIND_GRPC  = "grpc"
	REMOTE_KIND_HTTP  = "http"
	REMOTE_KIND_MYSQL = "mysql"
	REMOTE_KIND_REDIS = "redis"
)

func DumpSpans(spans []*traceproto.Span) ([]byte, error) {
	ds := &dumpproto.DumpSpans{Spans: spans}
	serialized, err := proto.Marshal(ds)
	if err != nil {
		return nil, err
	}
	return serialized, nil
}

func CreateSpans(num int) []*traceproto.Span {
	spans := make([]*traceproto.Span, 0, num*10)
	for i := 0; i < num; i++ {
		spans = append(spans, createSpanThread()...)
	}
	return spans
}

// simulate view a user's profile page
func createSpanThread() []*traceproto.Span {
	spans := make([]*traceproto.Span, 0, 10)

	// simulate call process
	// |----neo-api----------------------------------------------------------------------------------------------|
	//	  |---grpc client-------|
	//		|---grpc server---|
	//		  |--mysql client-|
	//								|---mysql client---|
	//														|---redis client---|
	//																			 |---redis client---|

	// neo api
	rootSpanInfo := &SpanInfo{
		serviceName:   "neo-api",
		operationName: "/api/user/:uid/profile",
		tags: map[string]interface{}{
			REMOTE_KIND: REMOTE_KIND_HTTP,
			"uid":       123456,
		},
		logs: []Log{
			Log{
				timestamp: time.Now(),
				annotations: map[string]interface{}{
					"query": "/api/user/123456/profile?from=web&version=1.0.1...",
				},
			},
		},
		delaySeconds: 0,
		costSeconds:  10,
		code:         0,
		message:      "ok",
	}
	rootSpan := createRootSpan(rootSpanInfo)
	spans = append(spans, rootSpan)

	// grpc client
	grpcClientSpanInfo := &SpanInfo{
		serviceName:   "neo-api",
		operationName: "GetUserProfile",
		tags: map[string]interface{}{
			REMOTE_KIND: REMOTE_KIND_GRPC,
			"uid":       123456,
			"source":    "web",
		},
		delaySeconds: 1,
		costSeconds:  4,
		code:         4,
		message:      "DeadlineExceeded",
	}
	grpcClientSpan := createChildSpanClient(rootSpan.TraceId, rootSpan.SpanId, grpcClientSpanInfo)
	spans = append(spans, grpcClientSpan)

	// grpc server
	grpcServerSpanInfo := &SpanInfo{
		serviceName:   "user-svc",
		operationName: "GetUserProfile",
		tags: map[string]interface{}{
			REMOTE_KIND: REMOTE_KIND_GRPC,
			"uid":       123456,
			"source":    "web",
		},
		delaySeconds: 2,
		costSeconds:  6,
		code:         0,
		message:      "ok",
	}
	grpcServerSpan := createChildSpanServer(rootSpan.TraceId, grpcClientSpan.SpanId, grpcServerSpanInfo)
	spans = append(spans, grpcServerSpan)

	// grpc server mysql client
	grpcMySQLClientSpanInfo := &SpanInfo{
		serviceName:   "user-svc",
		operationName: "select",
		tags: map[string]interface{}{
			REMOTE_KIND: REMOTE_KIND_MYSQL,
			"uid":       123456,
			"source":    "grpc",
		},
		logs: []Log{
			Log{
				timestamp: time.Now(),
				annotations: map[string]interface{}{
					"query": "select * from user where uid=123456",
				},
			},
		},
		delaySeconds: 3,
		costSeconds:  5,
		code:         0,
		message:      "ok",
	}
	grpcMySQLClientSpan := createChildSpanClient(rootSpan.TraceId, grpcServerSpan.SpanId, grpcMySQLClientSpanInfo)
	spans = append(spans, grpcMySQLClientSpan)

	// mysql client
	mysqlClientSpanInfo := &SpanInfo{
		serviceName:   "neo-api",
		operationName: "select",
		tags: map[string]interface{}{
			REMOTE_KIND: REMOTE_KIND_MYSQL,
			"uid":       123456,
			"source":    "web",
		},
		logs: []Log{
			Log{
				timestamp: time.Now(),
				annotations: map[string]interface{}{
					"query": "select * from profile where uid=123456",
				},
			},
		},
		delaySeconds: 9,
		costSeconds:  3,
		code:         0,
		message:      "ok",
	}
	mysqlClientSpan := createChildSpanClient(rootSpan.TraceId, rootSpan.SpanId, mysqlClientSpanInfo)
	spans = append(spans, mysqlClientSpan)

	// redis client
	redisClientSpanInfo := &SpanInfo{
		serviceName:   "neo-api",
		operationName: "mget",
		tags: map[string]interface{}{
			REMOTE_KIND: REMOTE_KIND_REDIS,
			"uid":       123456,
			"source":    "web",
			"count":     1000, // count stands for mget's param count
		},
		logs: []Log{
			Log{
				timestamp: time.Now(),
				annotations: map[string]interface{}{
					"query": "mget 1,2,3,4...",
				},
			},
		},
		delaySeconds: 9,
		costSeconds:  3,
		code:         0,
		message:      "ok",
	}
	redisClientSpan := createChildSpanClient(rootSpan.TraceId, rootSpan.SpanId, redisClientSpanInfo)
	spans = append(spans, redisClientSpan)

	return spans
}

type SpanInfo struct {
	serviceName   string
	operationName string
	tags          map[string]interface{}
	logs          []Log
	delaySeconds  int64
	costSeconds   int64
	code          int32
	message       string
}

type Log struct {
	timestamp   time.Time
	annotations map[string]interface{}
}

func createRootSpan(spanInfo *SpanInfo) *traceproto.Span {
	// pass service name through span attributes
	spanInfo.tags[SERVICE_NAME] = spanInfo.serviceName

	now := time.Now()
	traceId := hutils.IDGenerator.NewTraceID()
	spanId := hutils.IDGenerator.NewSpanID()

	return &traceproto.Span{
		TraceId: traceId[:],
		SpanId:  spanId[:],
		Name: &traceproto.TruncatableString{
			Value: spanInfo.operationName,
		},
		Kind:       traceproto.Span_SERVER,
		StartTime:  &timestamp.Timestamp{Seconds: now.Unix() + spanInfo.delaySeconds, Nanos: int32(now.UnixNano())},
		EndTime:    &timestamp.Timestamp{Seconds: now.Unix() + spanInfo.delaySeconds + spanInfo.costSeconds, Nanos: int32(now.UnixNano())},
		Attributes: convertTagsToAttributes(spanInfo.tags),
		TimeEvents: convertLogsToTimeEvents(spanInfo.logs),
		Status: &traceproto.Status{
			Code:    spanInfo.code,
			Message: spanInfo.message,
		},
	}
}

func createChildSpanClient(traceId []byte, parentSpanId []byte, spanInfo *SpanInfo) *traceproto.Span {
	return createChildSpan(traceId, parentSpanId, traceproto.Span_CLIENT, spanInfo)
}

func createChildSpanServer(traceId []byte, parentSpanId []byte, spanInfo *SpanInfo) *traceproto.Span {
	return createChildSpan(traceId, parentSpanId, traceproto.Span_SERVER, spanInfo)
}

func createChildSpan(traceId []byte, parentSpanId []byte, kind traceproto.Span_SpanKind, spanInfo *SpanInfo) *traceproto.Span {
	// pass service name through span attributes
	spanInfo.tags[SERVICE_NAME] = spanInfo.serviceName

	now := time.Now()
	spanId := hutils.IDGenerator.NewSpanID()

	return &traceproto.Span{
		TraceId:      traceId,
		SpanId:       spanId[:],
		ParentSpanId: parentSpanId[:],
		Name: &traceproto.TruncatableString{
			Value: spanInfo.operationName,
		},
		Kind:       kind,
		StartTime:  &timestamp.Timestamp{Seconds: now.Unix() + spanInfo.delaySeconds, Nanos: int32(now.UnixNano())},
		EndTime:    &timestamp.Timestamp{Seconds: now.Unix() + spanInfo.delaySeconds + spanInfo.costSeconds, Nanos: int32(now.UnixNano())},
		Attributes: convertTagsToAttributes(spanInfo.tags),
		TimeEvents: convertLogsToTimeEvents(spanInfo.logs),
		Status: &traceproto.Status{
			Code:    spanInfo.code,
			Message: spanInfo.message,
		},
	}
}

func convertTagsToAttributes(tags map[string]interface{}) *traceproto.Span_Attributes {
	attributes := &traceproto.Span_Attributes{
		AttributeMap: make(map[string]*traceproto.AttributeValue),
	}

	for k, i := range tags {
		switch v := i.(type) {
		case string:
			attributes.AttributeMap[k] = &traceproto.AttributeValue{
				Value: &traceproto.AttributeValue_StringValue{
					StringValue: &traceproto.TruncatableString{
						Value: v,
					},
				},
			}
		case bool:
			attributes.AttributeMap[k] = &traceproto.AttributeValue{
				Value: &traceproto.AttributeValue_BoolValue{
					BoolValue: v,
				},
			}
		case int:
			attributes.AttributeMap[k] = &traceproto.AttributeValue{
				Value: &traceproto.AttributeValue_IntValue{
					IntValue: int64(v),
				},
			}
		default:
			fmt.Printf("unknown tag value type:%v, ignored\n", v)
		}
	}

	return attributes
}

func convertLogsToTimeEvents(logs []Log) *traceproto.Span_TimeEvents {
	timeEvents := &traceproto.Span_TimeEvents{
		TimeEvent: make([]*traceproto.Span_TimeEvent, 0, 10),
	}
	for _, log := range logs {
		timeEvents.TimeEvent = append(timeEvents.TimeEvent,
			&traceproto.Span_TimeEvent{
				Time:  &timestamp.Timestamp{Seconds: log.timestamp.Unix(), Nanos: int32(log.timestamp.UnixNano())},
				Value: convertLogAnnoationToTimeEventAnnotation(log.annotations),
			},
		)
	}
	return timeEvents
}

func convertLogAnnoationToTimeEventAnnotation(annotations map[string]interface{}) *traceproto.Span_TimeEvent_Annotation_ {
	teAnnotation := &traceproto.Span_TimeEvent_Annotation_{
		Annotation: &traceproto.Span_TimeEvent_Annotation{
			Description: &traceproto.TruncatableString{
				Value: "user supplied log",
			},
			Attributes: convertTagsToAttributes(annotations),
		},
	}
	return teAnnotation
}

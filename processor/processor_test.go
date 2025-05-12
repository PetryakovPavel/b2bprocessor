package processor

import (
    "context"
    "testing"

    "go.opentelemetry.io/collector/consumer/consumertest"
    "go.opentelemetry.io/collector/pdata/ptrace"
    "go.uber.org/zap"

    "github.com/PetryakovPavel/b2bprocessor"
)

func TestDynamicBatchProcessor_FilterAndBatch(t *testing.T) {
    logger := zap.NewNop()
    cfg := &b2bprocessor.Config{
        IncludeHTTP:      true,
        IncludeGRPC:      false,
        InitialBatchSize: 2,
        MaxBatchSize:     10,
    }

    sink := new(consumertest.TracesSink)
    proc := NewDynamicBatchProcessor(cfg, logger, sink)

    td := ptrace.NewTraces()
    rs := td.ResourceSpans().AppendEmpty()
    ss := rs.ScopeSpans().AppendEmpty()

    for i := 0; i < 2; i++ {
        span := ss.Spans().AppendEmpty()
        span.SetName("http-span")
        span.Attributes().PutStr("http.method", "GET")
    }

    if err := proc.ConsumeTraces(context.Background(), td); err != nil {
        t.Fatalf("ConsumeTraces error: %v", err)
    }
    if sink.SpanCount() != 2 {
        t.Errorf("expected 2 spans, got %d", sink.SpanCount())
    }
}

func TestDynamicBatchProcessor_ExcludesGRPC(t *testing.T) {
    logger := zap.NewNop()
    cfg := &b2bprocessor.Config{
        IncludeHTTP:      false,
        IncludeGRPC:      false,
        InitialBatchSize: 1,
        MaxBatchSize:     10,
    }

    sink := new(consumertest.TracesSink)
    proc := NewDynamicBatchProcessor(cfg, logger, sink)

    td := ptrace.NewTraces()
    rs := td.ResourceSpans().AppendEmpty()
    ss := rs.ScopeSpans().AppendEmpty()
    span := ss.Spans().AppendEmpty()
    span.SetName("grpc-span")
    span.Attributes().PutStr("rpc.system", "grpc")

    if err := proc.ConsumeTraces(context.Background(), td); err != nil {
        t.Fatalf("ConsumeTraces error: %v", err)
    }
    if sink.SpanCount() != 0 {
        t.Errorf("expected 0 spans, got %d", sink.SpanCount())
    }
}

package processor

import (
    "context"
    "sync"
    "time"

    "go.opentelemetry.io/collector/consumer"
    "go.opentelemetry.io/collector/pdata/ptrace"
    "go.uber.org/zap"

    "github.com/your_username/b2bprocessor"
)

type DynamicBatchProcessor struct {
    cfg          *b2bprocessor.Config
    logger       *zap.Logger
    nextConsumer consumer.Traces

    mu        sync.Mutex
    batch     []ptrace.Span
    batchSize int
    lastPush  time.Time
}

func NewDynamicBatchProcessor(
    cfg *b2bprocessor.Config,
    logger *zap.Logger,
    next consumer.Traces,
) *DynamicBatchProcessor {
    return &DynamicBatchProcessor{
        cfg:          cfg,
        logger:       logger,
        nextConsumer: next,
        batchSize:    cfg.InitialBatchSize,
        lastPush:     time.Now(),
    }
}

// ConsumeTraces фильтрует спаны по http.method или rpc.system,
// накапливает их и отправляет батч при достижении порога.
func (p *DynamicBatchProcessor) ConsumeTraces(ctx context.Context, td ptrace.Traces) error {
    var filtered []ptrace.Span

    for i := 0; i < td.ResourceSpans().Len(); i++ {
        rs := td.ResourceSpans().At(i)
        for j := 0; j < rs.ScopeSpans().Len(); j++ {
            spans := rs.ScopeSpans().At(j).Spans()
            for k := 0; k < spans.Len(); k++ {
                span := spans.At(k)
                if isAllowed(span, p.cfg) {
                    filtered = append(filtered, span)
                }
            }
        }
    }
    if len(filtered) == 0 {
        return nil
    }

    p.mu.Lock()
    defer p.mu.Unlock()

    p.batch = append(p.batch, filtered...)
    if len(p.batch) >= p.batchSize {
        if err := p.sendBatch(ctx); err != nil {
            return err
        }
        p.adaptBatchSize()
    }
    return nil
}

func isAllowed(span ptrace.Span, cfg *b2bprocessor.Config) bool {
    attrs := span.Attributes()
    if val, ok := attrs.Get("rpc.system"); ok && val.Str() == "grpc" {
        return cfg.IncludeGRPC
    }
    if _, ok := attrs.Get("http.method"); ok {
        return cfg.IncludeHTTP
    }
    return false
}

func (p *DynamicBatchProcessor) sendBatch(ctx context.Context) error {
    td := ptrace.NewTraces()
    rs := td.ResourceSpans().AppendEmpty()
    ss := rs.ScopeSpans().AppendEmpty()
    spans := ss.Spans()

    for _, span := range p.batch {
        newSpan := spans.AppendEmpty()
        span.CopyTo(newSpan)
    }

    p.batch = nil
    p.lastPush = time.Now()
    return p.nextConsumer.ConsumeTraces(ctx, td)
}

func (p *DynamicBatchProcessor) adaptBatchSize() {
    elapsed := time.Since(p.lastPush)
    if elapsed < time.Second && p.batchSize < p.cfg.MaxBatchSize {
        p.batchSize += 10
    } else if elapsed > 2*time.Second && p.batchSize > 10 {
        p.batchSize -= 10
    }
}

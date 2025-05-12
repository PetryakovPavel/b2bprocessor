package b2bprocessor

// Config держит все настройки вашего процессора.
type Config struct {
    IncludeHTTP     bool // включать HTTP-спаны
    IncludeGRPC     bool // включать gRPC-спаны
    InitialBatchSize int  // начальный размер батча
    MaxBatchSize     int  // максимальный размер батча
}

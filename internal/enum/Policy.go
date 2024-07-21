package policy

type UploadPolicy int

const (
    Overwrite UploadPolicy = iota
    VersionControl
    Deduplication
    IncrementalUpdate
)
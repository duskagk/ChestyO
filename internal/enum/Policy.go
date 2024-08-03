package enum

type UploadPolicy int

const (
	Overwrite UploadPolicy = iota
	VersionControl
	NoChange
)

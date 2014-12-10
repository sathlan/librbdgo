package rbd

func (img *Image) setSnapshotName(name string) error {
	img.snapshot = name
	img.wantSnapshot = true
	return nil
}

func (img *Image) setReadOnly() error {
	img.readOnly = true
	return nil
}

// SnapshotName is a configuration option for Image.
func SnapshotName(name string) func(*Image) error {
	return func(img *Image) error {
		return img.setSnapshotName(name)
	}
}

// ReadOnly is a configuration option for Image.
func ReadOnly(img *Image) error {
	return img.setReadOnly()
}

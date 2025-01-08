package fsrepo

import "testing"

func TestDiskSpec_String(t *testing.T) {
	spec := DefaultDiskSpec()
	str := spec.String()
	t.Logf("DiskSpec.String() = %s", str)
}

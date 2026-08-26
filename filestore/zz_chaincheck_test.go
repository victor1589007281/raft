package filestore

import "testing"

// Placeholder for a manual chain-error probe used while fixing the io_uring
// SQE ABI; kept disabled. Real coverage: ioring_test.go.
func TestChainErrSurface(t *testing.T) { t.Skip("manual probe artifact") }

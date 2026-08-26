package filestore

import "testing"

// Placeholder for a manual io_uring probe used during development of the
// dual-fd chain; kept disabled. See TestRefIORingDual for the real coverage.
func TestIOProbeDualReal(t *testing.T) { t.Skip("manual probe artifact") }

package filestore

import "testing"

// Placeholder for the manual per-CQE drain probe used while diagnosing the
// SQE ABI bug; kept disabled. Real coverage: TestIORingDualChainLandsBothFiles.
func TestChainPerCQE(t *testing.T) { t.Skip("manual probe artifact") }

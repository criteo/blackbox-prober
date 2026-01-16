package triton

// ComputeModelActivity determines if a model is active based on execution counts.
// Returns true if external executions (beyond expected probe traffic) exceed the margin.
//
// Parameters:
//   - prevExecCount: execution count from previous refresh
//   - currExecCount: current execution count from Triton
//   - expectedProbeCount: expected number of probes (replicas × probes per refresh)
//   - margin: minimum external executions required to consider the model active
//
// Returns true if the model is considered active.
func ComputeModelActivity(prevExecCount, currExecCount, expectedProbeCount uint64, margin int64) bool {
	// Handle counter reset (e.g., server restart)
	if currExecCount < prevExecCount {
		return true
	}

	delta := currExecCount - prevExecCount
	external := int64(delta) - int64(expectedProbeCount)

	return external > margin
}

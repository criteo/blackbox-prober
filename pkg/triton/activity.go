package triton

// ComputeModelActivity determines if a model is active based on execution counts.
// A model is considered active if there are external executions (beyond probe traffic) above the margin.
//
// Parameters:
//   - prevExecCount: execution count from previous refresh (0 if first observation)
//   - currExecCount: current execution count from Triton
//   - probeCount: number of probes we made since last refresh
//   - margin: minimum external executions required to consider the model active
//   - isFirstObservation: true if this is the first time we're seeing this model
//
// Returns true if the model is considered active.
func ComputeModelActivity(prevExecCount, currExecCount, probeCount uint64, margin int64, isFirstObservation bool) bool {
	// First observation - no baseline, assume active
	if isFirstObservation {
		return true
	}

	// Handle counter reset (e.g., server restart)
	if currExecCount < prevExecCount {
		return true
	}

	deltaExecutions := currExecCount - prevExecCount
	externalExecutions := int64(deltaExecutions) - int64(probeCount)

	return externalExecutions > margin
}


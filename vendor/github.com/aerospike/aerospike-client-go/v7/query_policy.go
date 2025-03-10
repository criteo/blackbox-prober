// Copyright 2014-2022 Aerospike, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package aerospike

// QueryPolicy encapsulates parameters for policy attributes used in query operations.
type QueryPolicy struct {
	MultiPolicy

	// Expected query duration. The server treats the query in different ways depending on the expected duration.
	// This field is ignored for aggregation queries, background queries and server versions < 6.0.
	//
	// Default: LONG
	ExpectedDuration QueryDuration

	// ShortQuery determines wether query expected to return less than 100 records.
	// If true, the server will optimize the query for a small record set.
	// This field is ignored for aggregation queries, background queries
	// and server versions 6.0+.
	//
	// Default: false
	// This field is deprecated and will eventually be removed. Use ExpectedDuration instead.
	// For backwards compatibility: If ShortQuery is true, the query is treated as a short query and
	// ExpectedDuration is ignored. If shortQuery is false, ExpectedDuration is used defaults to {@link QueryDuration#LONG}.
	ShortQuery bool
}

// NewQueryPolicy generates a new QueryPolicy instance with default values.
// Set MaxRetries for non-aggregation queries with a nil filter on
// server versions >= 4.9. All other queries are not retried.
//
// The latest servers support retries on individual data partitions.
// This feature is useful when a cluster is migrating and partition(s)
// are missed or incomplete on the first query (with nil filter) attempt.
//
// If the first query attempt misses 2 of 4096 partitions, then only
// those 2 partitions are retried in the next query attempt from the
// last key digest received for each respective partition. A higher
// default MaxRetries is used because it's wasteful to invalidate
// all query results because a single partition was missed.
func NewQueryPolicy() *QueryPolicy {
	return &QueryPolicy{
		MultiPolicy: *NewMultiPolicy(),
	}
}

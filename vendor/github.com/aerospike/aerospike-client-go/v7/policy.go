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

import (
	"time"
)

// Policy Interface
type Policy interface {
	// Retrieves BasePolicy
	GetBasePolicy() *BasePolicy

	// determines if the command should be compressed
	compress() bool
}

// enforce the interface
var _ Policy = &BasePolicy{}

// BasePolicy encapsulates parameters for transaction policy attributes
// used in all database operation calls.
type BasePolicy struct {
	// FilterExpression is the optional Filter Expression. Supported on Server v5.2+
	FilterExpression *Expression

	// ReadModeAP indicates read policy for AP (availability) namespaces.
	ReadModeAP ReadModeAP //= ONE

	// ReadModeSC indicates read policy for SC (strong consistency) namespaces.
	ReadModeSC ReadModeSC //= SESSION;

	// TotalTimeout specifies total transaction timeout.
	//
	// The TotalTimeout is tracked on the client and also sent to the server along
	// with the transaction in the wire protocol. The client will most likely
	// timeout first, but the server has the capability to Timeout the transaction.
	//
	// If TotalTimeout is not zero and TotalTimeout is reached before the transaction
	// completes, the transaction will abort with TotalTimeout error.
	//
	// If TotalTimeout is zero, there will be no time limit and the transaction will retry
	// on network timeouts/errors until MaxRetries is exceeded. If MaxRetries is exceeded, the
	// transaction also aborts with Timeout error.
	//
	// Default for scan/query: 0 (no time limit and rely on MaxRetries)
	//
	// Default for all other commands: 1000ms
	TotalTimeout time.Duration

	// SocketTimeout determines network timeout for each attempt.
	//
	// If SocketTimeout is not zero and SocketTimeout is reached before an attempt completes,
	// the Timeout above is checked. If Timeout is not exceeded, the transaction
	// is retried. If both SocketTimeout and Timeout are non-zero, SocketTimeout must be less
	// than or equal to Timeout, otherwise Timeout will also be used for SocketTimeout.
	//
	// Default: 30s
	SocketTimeout time.Duration

	// MaxRetries determines the maximum number of retries before aborting the current transaction.
	// The initial attempt is not counted as a retry.
	//
	// If MaxRetries is exceeded, the transaction will abort with an error.
	//
	// WARNING: Database writes that are not idempotent (such as AddOp)
	// should not be retried because the write operation may be performed
	// multiple times if the client timed out previous transaction attempts.
	// It's important to use a distinct WritePolicy for non-idempotent
	// writes which sets maxRetries = 0;
	//
	// Default for read: 2 (initial attempt + 2 retries = 3 attempts)
	//
	// Default for write: 0 (no retries)
	//
	// Default for partition scan or query with nil filter: 5
	// (6 attempts. See ScanPolicy comments.)
	MaxRetries int //= 2;

	// ReadTouchTTLPercent determines how record TTL (time to live) is affected on reads. When enabled, the server can
	// efficiently operate as a read-based LRU cache where the least recently used records are expired.
	// The value is expressed as a percentage of the TTL sent on the most recent write such that a read
	// within this interval of the record’s end of life will generate a touch.
	//
	// For example, if the most recent write had a TTL of 10 hours and read_touch_ttl_percent is set to
	// 80, the next read within 8 hours of the record's end of life (equivalent to 2 hours after the most
	// recent write) will result in a touch, resetting the TTL to another 10 hours.
	//
	// Values:
	//
	// 0 : Use server config default-read-touch-ttl-pct for the record's namespace/set.
	// -1 : Do not reset record TTL on reads.
	// 1 - 100 : Reset record TTL on reads when within this percentage of the most recent write TTL.
	// Default: 0
	ReadTouchTTLPercent int32

	// SleepBetweenRtries determines the duration to sleep between retries.  Enter zero to skip sleep.
	// This field is ignored when maxRetries is zero.
	// This field is also ignored in async mode.
	//
	// The sleep only occurs on connection errors and server timeouts
	// which suggest a node is down and the cluster is reforming.
	// The sleep does not occur when the client's socketTimeout expires.
	//
	// Reads do not have to sleep when a node goes down because the cluster
	// does not shut out reads during cluster reformation.  The default for
	// reads is zero.
	//
	// The default for writes is also zero because writes are not retried by default.
	// Writes need to wait for the cluster to reform when a node goes down.
	// Immediate write retries on node failure have been shown to consistently
	// result in errors.  If maxRetries is greater than zero on a write, then
	// sleepBetweenRetries should be set high enough to allow the cluster to
	// reform (>= 500ms).
	SleepBetweenRetries time.Duration //= 1ms;

	// SleepMultiplier specifies the multiplying factor to be used for exponential backoff during retries.
	// Default to (1.0); Only values greater than 1 are valid.
	SleepMultiplier float64 //= 1.0;

	// ExitFastOnExhaustedConnectionPool determines if a command that tries to get a
	// connection from the connection pool will wait and retry in case the pool is
	// exhausted until a connection becomes available (or the TotalTimeout is reached).
	// If set to true, an error will be return immediately.
	// If set to false, getting a connection will be retried.
	// This only applies if LimitConnectionsToQueueSize is set to true and the number of open connections to a node has reached ConnectionQueueSize.
	// The default is false
	ExitFastOnExhaustedConnectionPool bool // false

	// SendKey determines to whether send user defined key in addition to hash digest on both reads and writes.
	// If the key is sent on a write, the key will be stored with the record on
	// the server.
	//
	// If the key is sent on a read, the server will generate the hash digest from
	// the key and validate that digest with the digest sent by the client. Unless
	// this is the explicit intent of the developer, avoid sending the key on reads.
	// The default is to not send the user defined key.
	SendKey bool // = false

	// UseCompression uses zlib compression on command buffers sent to the server and responses received
	// from the server when the buffer size is greater than 128 bytes.
	//
	// This option will increase cpu and memory usage (for extra compressed buffers),but
	// decrease the size of data sent over the network.
	//
	// Default: false
	UseCompression bool // = false

	// ReplicaPolicy specifies the algorithm used to determine the target node for a partition derived from a key
	// or requested in a scan/query.
	// Write commands are not affected by this setting, because all writes are directed
	// to the node containing the key's master partition.
	// Default to sending read commands to the node containing the key's master partition.
	ReplicaPolicy ReplicaPolicy
}

// NewPolicy generates a new BasePolicy instance with default values.
func NewPolicy() *BasePolicy {
	return &BasePolicy{
		ReadModeAP:          ReadModeAPOne,
		ReadModeSC:          ReadModeSCSession,
		TotalTimeout:        1000 * time.Millisecond,
		SocketTimeout:       30 * time.Second,
		MaxRetries:          2,
		SleepBetweenRetries: 1 * time.Millisecond,
		SleepMultiplier:     1.0,
		ReplicaPolicy:       SEQUENCE,
		SendKey:             false,
		UseCompression:      false,
	}
}

var _ Policy = &BasePolicy{}

// GetBasePolicy returns embedded BasePolicy in all types that embed this struct.
func (p *BasePolicy) GetBasePolicy() *BasePolicy { return p }

func (p *BasePolicy) deadline() time.Time {
	var deadline time.Time
	if p != nil && p.TotalTimeout > 0 {
		deadline = time.Now().Add(p.TotalTimeout)
	}

	return deadline
}

func (p *BasePolicy) compress() bool {
	return p.UseCompression
}

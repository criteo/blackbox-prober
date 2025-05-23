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

type batcher interface {
	command

	cloneBatchCommand(batch *batchNode) batcher
	filteredOut() int

	retryBatch(ifc batcher, cluster *Cluster, iteration int) (bool, Error)
	generateBatchNodes(*Cluster) ([]*batchNode, Error)
	setSequence(int, int)

	executeSingle(clientIfc) Error
}

type clientIfc interface {
	ClientIfc

	operate(*WritePolicy, *Key, bool, ...*Operation) (*Record, Error)
	execute(policy *WritePolicy, key *Key, packageName string, functionName string, args ...Value) (*Record, Error)
}

type batchCommand struct {
	baseMultiCommand

	client     clientIfc
	batch      *batchNode
	policy     *BatchPolicy
	sequenceAP int
	sequenceSC int

	splitRetry bool

	filteredOutCnt int
}

func (cmd *batchCommand) prepareRetry(ifc command, isTimeout bool) bool {
	if !(cmd.policy.ReplicaPolicy == SEQUENCE || cmd.policy.ReplicaPolicy == PREFER_RACK) {
		// Perform regular retry to same node.
		return true
	}

	cmd.sequenceAP++

	if !isTimeout || cmd.policy.ReadModeSC != ReadModeSCLinearize {
		cmd.sequenceSC++
	}
	return false
}

func (cmd *batchCommand) retryBatch(ifc batcher, cluster *Cluster, iteration int) (bool, Error) {
	// Retry requires keys for this node to be split among other nodes.
	// This is both recursive and exponential.
	batchNodes, err := ifc.generateBatchNodes(cluster)
	if err != nil {
		return false, err
	}

	if len(batchNodes) == 1 && batchNodes[0].Node == cmd.batch.Node {
		// Batch node is the same. Go through normal retry.
		return false, nil
	}

	// Run batch requests sequentially in same thread.
	var ferr Error
	for _, batchNode := range batchNodes {
		command := ifc.cloneBatchCommand(batchNode)
		command.setSequence(cmd.sequenceAP, cmd.sequenceSC)
		if err := command.executeAt(command, cmd.policy.GetBasePolicy(), cmd.policy.deadline(), iteration); err != nil {
			ferr = chainErrors(err, ferr)
			if !cmd.policy.AllowPartialResults {
				return false, ferr
			}
		}
	}

	return true, ferr
}

func (cmd *batchCommand) setSequence(ap, sc int) {
	cmd.sequenceAP, cmd.sequenceSC = ap, sc
}

func (cmd *batchCommand) getPolicy(ifc command) Policy {
	return cmd.policy
}

func (cmd *batchCommand) transactionType() transactionType {
	return ttNone
}

func (cmd *batchCommand) Execute() Error {
	return cmd.execute(cmd)
}

func (cmd *batchCommand) filteredOut() int {
	return cmd.filteredOutCnt
}

func (cmd *batchCommand) generateBatchNodes(cluster *Cluster) ([]*batchNode, Error) {
	panic(unreachable)
}

func (cmd *batchCommand) cloneBatchCommand(batch *batchNode) batcher {
	panic(unreachable)
}

func (cmd *batchCommand) writeBuffer(ifc command) Error {
	panic(unreachable)
}

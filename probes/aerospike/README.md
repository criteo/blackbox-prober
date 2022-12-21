# General

## Namespace discovery

The probe is not discovering namespaces automatically. The `namespace_meta_key`
needs to be defined on the cluster's consul services for the probe to discover them
The probe will not do any checking without at least one namespace specified.

Example:
`namespace_meta_key` per default is set to `aerospike-namespaces`. The value
is a set of namespace name separated by a semicolon `;`.  

Example: advertising "foo" and "bar" namespace

`aerospike-namespaces: "foo;bar"`


## Latency checks executed at cluster level

In the Aerospike probe, all latency checks are being run on cluster level. Normally
they should be done at node level but with Aerospike it is not simple to write
on a specifc node/partion as it depends heavily on consistent hashing.
Instead we execute the check at cluster level and try to find were the request
will be performed (by looking at current topology). It is not perfect but
it was the best compromise at the time.


## One "cluster" for each Namespace

In Aerospike, Namespace are mostly isolated (using difference disks, even different
nodes). It means that different namespace on the same node might have a different
behavior. The probe will query each namespace independently.

The second interest architectural: the probe-worker can only start if the prepare
phase is complete. At the moment, the prepare phase is used to create all the
objects for the durability check. If it starts without all objects created it can
trigger false positives. We chose to prevent a worker from starting if the prepare
phase is not working. The prepare phase is executed periodically until it complete,
or human intervention (the scheduled failed metric will incr at each failed prepare).

Considering different namespaces as different clusters is a simple way to enable
namespace checking to start independantly.


# Checks

## Latency

The latency check is performing the following operations:
- Put an object with some data
- Get the object and read its data and compare with what was put earlier
- Delete the object

We use some heuristics to guess which node processed the request. While it may not
be 100% accurate, having latency per server is very useful for debugging.

## Durability

The durability check is working by writing many item once and checking if they
are still there and if their data is correct.

It is done in two phases: 

#### Prepare phase

Executed at the startup of the probe:
- look for a flag item to determine if data has already been pushed
- if not present: push all the items requested


#### Check phase

Executed every X period of time:
- Look for all items and validate the data for each one

## Fixing the data after dataloss

To reset the data after a loss the simpliest is to remove the flag item
(named: durability_key_prefix + "all_pushed_flag") or to change the number of items to be used
by the probe.

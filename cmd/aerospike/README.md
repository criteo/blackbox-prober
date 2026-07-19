# General

## Namespace discovery

The probe is not discovering namespaces automatically. The `namespace_meta_key_prefix`
needs to be defined on the cluster's consul services for the probe to discover them
The probe will not do any checking without at least one namespace specified.

Example:
`namespace_meta_key_prefix` per default is set to `aerospike-monitoring-`.

For advertising "foo" and "bar" namespace

`aerospike-monitoring-foo: true`
`aerospike-monitoring-bar: true`

## Latency checks executed at cluster level

In the Aerospike probe, all latency checks are being run on cluster level. Normally
they should be done at node level but with Aerospike it is not simple to write
on a specifc node/partion as it depends heavily on consistent hashing.
Instead we execute the check at cluster level and try to find were the request
will be performed (by looking at current topology). It is not perfect but
it was the best compromise at the time.


## Namespace execution

In Aerospike, Namespace are mostly isolated (using difference disks, even different
nodes). It means that different namespace on the same node might have a different
behavior. The probe will query each namespace independently.

The probe creates one endpoint and one Aerospike client per cluster. Checks iterate
over the monitored namespaces with bounded parallelism. The durability prepare
phase runs namespaces sequentially to avoid creating a startup write burst.

The probe-worker can only start if the prepare phase is complete. At the moment,
the prepare phase is used to create all the objects for the durability check. If
it starts without all objects created it can trigger false positives. We prevent a
worker from starting if the prepare phase is not working. The prepare phase is
retried on topology updates until it completes or until human intervention.


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

## Auth

The auth check verifies that a brand-new connection can still authenticate on
each live Aerospike node. This catches failures masked by the long-lived client
session used by latency and durability checks.

Authentication rejections fail the scheduler check. Connection errors are counted
with `status="connection_error"` but do not fail the auth check, so scheduler
failures for `auth_check` stay specific to authentication.

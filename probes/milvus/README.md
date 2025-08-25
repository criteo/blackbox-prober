# General

## database discovery

The probe is not discovering databases automatically. The `database_meta_key_prefix`
needs to be defined on the cluster's consul services for the probe to discover them
The probe will not do any checking without at least one database specified.

Example:
`database_meta_key_prefix` per default is set to `milvus-monitoring-`.

For advertising "foo" and "bar" database

`milvus-monitoring-foo: true`
`milvus-monitoring-bar: true`

## Latency checks executed at cluster level

In the Aerospike probe, all latency checks are being run on cluster level. Normally
they should be done at node level but with Aerospike it is not simple to write
on a specifc node/partion as it depends heavily on consistent hashing.
Instead we execute the check at cluster level and try to find were the request
will be performed (by looking at current topology). It is not perfect but
it was the best compromise at the time.

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

# Cloner

Cloner is a Golang app that is run as a task inside the application namespace so that it can access the apps mTLS certificates.

There are some Cash specific assumptions right now but it could eventually become completely generic and work across Vitess, TiDB, Aurora and MySQL.

## Goals

It needs to solve for the following:
 * Minimize bandwidth required from onprem to cloud as this is a limited resource
 * Minimize the amount of writes required to "rebuild"/"repair" a clone if replication is broken - it's much more expensive to write
 * At some point be open sourceable so not too many Cash specific hard wired assumptions (there will be a few of those though)

## Assumptions

 * Single integer id primary key (we could relax this restriction but it would complicate the code tremendously)
 * Single integer sharding column, could be loaded from the vschema but will be hardcoded for Franklin to start with at least (it's almost always `customer_id` for new tables but some older tables have other sharding columns)
 
## Algorithm

It performs a diffing clone like this:
 1. Connects to a single Franklin on prem shard either via franklin-vtgate or via the ODS ingress envoy (below)
 2. Connect to the connects to TiDB with a sharding filter such that it only sees a single Franklin shard at the time of TiDB
 3. For each table
     1. Chunk up each table in roughly equally sized parts (see below for details)
     2. Diff the table to generate a bunch of diffs (see below for details)
     3. Batch up the diffs by type (inserts, updates or deletes)
     4. Send off the batches to writers

Writers and differs run in parallel in a pool so that longer tables are diffed and written in parallel.
     
## Chunking a table

Ideally we'd use the following windowing SQL:
```
select
	  min(tmp.id) as start_rowid,
	  max(tmp.id) as end_rowid,
	  count(*) as page_size
from (
  select
	id,
	row_number() over (order by id) as row_num
  from %s
) tmp
group by floor((tmp.row_num - 1) / ?)
order by start_rowid
``` 

But sadly windowing functions are not supported by Vitess so we have to do a rather silly thing where we select the IDs ordered by ID and then split that up into equal parts. [See the code](https://github.com/squareup/cloner/blob/master/pkg/clone/chunker.go#L115) for more detail.

Ideally we would use the MySQL built in stats histogram of the primary key index of a table but not sure that is easily accessible.

## Diffing a chunk

We filter the TiDB side query by Vitess shard so we can clone a single Vitess shard at the time without deleting a bunch of out-of-shard data. This filtering should use the MySQL built in DES_ENCRYPT/DES_DECRYPT but sadly that is currently not yet supported by TiDB. So we do this filtering client side. [There is a ticket to implement this](https://github.com/pingcap/tidb/issues/4055) and it would not be a lot of work.

First we should execute a single SQL statement that calculates a checksum. We execute the same statement on both TiDB and MySQL and compare the results. Only if the checksum doesn't match we go to the real diffing part. This is to minimize the bandwidth required. Sadly, this is not yet implemented.

After that we stream the results in parallel and execute a simple stream diffing algorithm. ([See code](https://github.com/squareup/cloner/blob/master/pkg/clone/differ.go#L59) for more detail.)

## Checksumming

This tool can also be used to checksum across a replication boundary without any freezing in time.

```puml
database A
database B

A -r-> B : replication
```

We divide each table into chunks as above. Then for each chunk we calculate the checksum and compare. If there is a difference it can mean two things:
 1. There is data corruption in that chunk or,
 2. there is replication lag for that chunk

In order to differentiate between the two possibilities we simply re-run the checksum a fixed amount of time. If there is replication lag for that chunk it should generally resolve after a few retries. If not, it's likely there is data corruption.

## Consistent clone

To be able to start replication we have to clone the database at a specific GTID. To do that we have to either:
* Create multiple synced connections using either `LOCK TABLES ... READ` or `FLUSH TABLES WITH READ LOCK` [see more in the dumpling codebase](https://github.com/pingcap/dumpling/blob/b84f64ff362cedcb795aa23fa1188ba7b7c9a7d7/v4/export/consistency.go#L22)
* This might not be exposed to vtgate in which case we can use a single connection with a read transaction. This will be a lot slower since we won't have any parallelism but if we do a few inconsistent clones before to minimize the delta to write it might be fine

This is not currently implemented.

## Open source

There are some Cash specific assumption that we would need to remove to make it possible to make it open source:

 * Support arbitrary primary keys
 * Support a configurable WHERE clause per table on either side of the clone - this would be to support the TiDB side shard filtering. This configuration could be generated from the vschema. I think it would be difficult to support arbitrary vindexes so we assume the Vitess `hash` vindex 

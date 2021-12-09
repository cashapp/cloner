# Cloner

Cloner is a Golang app that clones a database to another database by diffing both databases and writing the minimal amount inserts/updates/deletes to make them identical. It can also (eventually) be used to checksum two databases where one is replicated into the other without stopping replication.
 
There are some Cash specific assumptions right now but it could eventually become completely generic and work across Vitess, TiDB, Aurora and MySQL or any database that has a Go sql driver.

## Goals

It needs to solve for the following:
 * Minimize bandwidth required from onprem to cloud as this is a limited resource
 * Minimize the amount of writes required to "rebuild"/"repair" a clone if replication is broken - it's much more expensive to write
 * At some point be open sourceable so not too many Cash specific hard wired assumptions (there will be a few of those though)

## Best effort cloning

It performs a diffing clone ("repair") like this:
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

We could use the MySQL built in stats histogram of the primary key index of a table but not sure that is easily accessible.

## Diffing a chunk

We filter the TiDB side query by Vitess shard using a configurable where clause so we can clone a single Vitess shard at the time without deleting a bunch of out-of-shard data.

After that we load the result set of both the source and the target and perform a simple diffing algorithm. ([See code](https://github.com/squareup/cloner/blob/master/pkg/clone/differ.go#L59) for more detail.)

(We could use the same approach for calculating a chunk checksum as pt-table-checksum but this is computationally intensive on TiDB right now so instead we just load all the rows of a chunks and compare in memory. https://github.com/percona/percona-toolkit/blob/3.x/lib/TableChecksum.pm#L367)

## Checksumming

This tool can be used to verify replication integrity without any freezing in time.

We divide each table into chunks as above. Then we load each chunk from source and target and compare. If there is a difference it can mean two things: 1) There is data corruption in that chunk or, 2) there is replication lag for that chunk

In order to differentiate between these two possibilities we simply re-load the chunk data and compare again a fixed amount of time. If there is replication lag for that chunk it should generally resolve after a few retries. If not, it's likely there is data corruption.

## Replication

Reads the MySQL binlog and applies to the target.

Writes checkpoints to a checkpoint table in the target. Restarts from the checkpoint if present.

Does not currently support DDL.

## Determine end to end replication lag

Writes to a heartbeat table and then read the heartbeat table from the source. This determines real end to end replication lag (with the heartbeat period as resolution). It's published as a Prometheus metric.

## Consistent clone

Consistent cloning uses Netflix DBLog algorithm as presented in this paper:
https://arxiv.org/pdf/2010.12597.pdf

In summary the consistent cloning algorithm works by this:
1. Start replication
2. Chunk each table as above
3. For each chunk: write a low watermark to a watermark table in the source, then read the chunk rows then write a high watermark.
4. When the replication loop encounters a low watermark it starts reconciling any binlog events inside the chunk with the in memory result set of the chunk. (Replaces rows that are updated or inserted and deletes rows that are deleted.)
5. When the replication loop encounters a high watermark the chunk is now strongly consistent with the source and is diffed and written (as described above).

It needs write access to the source to be able to write to the watermark table.

## Parallel replication

Runs transactions in parallel unless they are causal. Transactions A and B are causal iff 1) A happens before transaction B in the global ordering and 2) the set of primary keys they write to overlap.

The primary key set of a chunk "repair" (i.e. diffing/writing of a chunk at the point of the high watermark) is the full primary key set of that chunk, i.e. a chunk repair never runs in parallel with any other transactions inside that chunk.

Rough algorithm:
1. Gather transactions until we have N transactions or a configurable amount of time passes
2. Calculate causal chains of all transactions which produces a set of transaction sequences
3. Apply all transaction sequences in parallel, once all have been applied write the last replication position to the checkpoint table in the target  
4. Repeat from 1 in parallel with 3 but don't start applying transactions until the previous transactions have completed and the checkpoint table has been written to

Parallel replication can encounter partial failure as in some transactions may be written before the checkpoint table is written to. In this case when replication starts over again some transactions are re-applied. Fortunately the way we apply a transaction is idempotent.

## Open sourcing status

Cloner will be open sourced once all Cash specific features have been removed.

Only a single Cash specific feature is left: Connecting using a Misk datasource. This can probably be replaced with command line flags and support for JKS/JCEKS key stores.

# Cloner

Cloner is a Golang app that clones a database to another database. It supports:
 * Best effort cloning
 * Replication (including heartbeating and safe parallelism)
 * Checksumming
 * Consistent cloning

None of these algorithms require stopping replication.

It is currently mainly focused on migrating from sharded or unsharded MySQL (including Aurora) to TiDB but can eventually be used to migrate from/to any SQL database that has a Go driver.

## Best effort cloning

Best effort cloning performs a diffing clone ("repair") like this:

 1. [Chunk up each table in roughly equally sized parts](https://github.com/cashapp/cloner/blob/master/pkg/clone/chunker.go)
 2. [Diff the table to generate a bunch of diffs](https://github.com/cashapp/cloner/blob/master/pkg/clone/differ.go)
 3. Batch up the diffs by type (inserts, updates or deletes)
 4. Send off the batches to writers

Writers and differs run in parallel in a pool so that longer tables are diffed and written in parallel.

## Checksumming

This tool can be used to verify replication integrity without any freezing in time (stopping replication).

We divide each table into chunks as above. Then we load each chunk from source and target and compare. If there is a difference it can mean two things: 1) There is data corruption in that chunk or, 2) there is replication lag for that chunk

In order to differentiate between these two possibilities we simply re-load the chunk data and compare again a after fixed amount of time. If there is replication lag for that chunk it should generally resolve after a few retries. If not, it's likely there is data corruption.

(There is an option to same approach for calculating a chunk checksum as pt-table-checksum but this is computationally intensive on TiDB right now for some reason so it's faster to not do this. https://github.com/percona/percona-toolkit/blob/3.x/lib/TableChecksum.pm#L367)

## Replication

Reads the MySQL binlog and applies to the target.

Writes checkpoints to a checkpoint table in the target. Restarts from the checkpoint if present.

Does not currently support DDL. If you need to do DDL then stop replication and delete the checkpoint row, run the DDL, then restart replication and run another consistent clone to repair.

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

Apples transactions in parallel unless they are causal. Transactions A and B are causal iff 1) A happens before transaction B in the global ordering and 2) the set of primary keys they write to overlap.

The primary key set of a chunk "repair" (i.e. diffing/writing of a chunk at the point of the high watermark) is the full primary key set of that chunk, i.e. a chunk repair never runs in parallel with any other transactions inside that chunk.

Rough algorithm:
1. Gather transactions until we have N transactions or a configurable amount of time passes
2. Calculate causal chains of all transactions which produces a set of transaction sequences
3. Apply all transaction sequences in parallel, once all have been applied write the last replication position to the checkpoint table in the target  
4. Repeat from 1 in parallel with 3 but don't start applying transactions until the previous transactions have completed and the checkpoint table has been written to

Parallel replication can encounter partial failure as in some transactions may be written before the checkpoint table is written to. In this case when replication starts over again some transactions are re-applied. Fortunately the way we apply a transaction is idempotent.

## Sharded cloning

Cloner supports merging sharded databases for all the algorithms above. We filter the target side query by shard using a configurable where clause so we can clone/checksum a single shard at the time without deleting a bunch of out-of-shard data.

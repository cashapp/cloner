# Tutorial: Migrating from one database to another

These are all long running operations and are best run as for example Kubernetes `job` for the finite jobs such as best effort clones and checksums or `deployment` for the replication/consistent clones.

## 1. Make sure your app works in the target database

This migration process is not reversible without some data loss. There is no replication from the target database to the source, so if the replication is reversed any writes that were sent to the target won't automatically be replicated back to the source. They can however be manually copied back.

For this reason it's a good idea to make sure your app runs well on your target database before you try the migration. For example:
* Make sure your CI tests run on the target database.
* After doing some best effort clonses (step 3 below) replay some shadow queries using for exampler [Chronicler](https://github.com/cashapp/chronicler).

## 2. Copy the schema

cloner does not currently copy the schema, this needs to be done by either running your migrations or simply dumping and copying. This is a good time to do any modifications you might need to the schema.

At this point you could also create the worker tables for cloner. Cloner can create them automatically but sometimes the user used by cloner does not have access to run CREATE TABLE statements.

In the source database create the heartbeat and the watermark tables:
```
CREATE TABLE IF NOT EXISTS _cloner_heartbeat (
    name VARCHAR(255) NOT NULL,
    time TIMESTAMP NOT NULL,
    count BIGINT(20) NOT NULL,
    PRIMARY KEY (name)
);

CREATE TABLE IF NOT EXISTS _cloner_watermark (
    id         BIGINT(20)   NOT NULL AUTO_INCREMENT,
    table_name VARCHAR(255) NOT NULL,
    chunk_seq  BIGINT(20)   NOT NULL,
    low        TINYINT      DEFAULT 0,
    high       TINYINT      DEFAULT 0,
    PRIMARY KEY (id)
);
```

In the target database create the heartbeat and the checkpoint tables:
```
CREATE TABLE IF NOT EXISTS _cloner_heartbeat (
    name VARCHAR(255) NOT NULL,
    time TIMESTAMP NOT NULL,
    count BIGINT(20) NOT NULL,
    PRIMARY KEY (name)
);

CREATE TABLE IF NOT EXISTS _cloner_checkpoint (
    name      VARCHAR(255) NOT NULL,
    file      VARCHAR(255) NOT NULL,
    position  BIGINT(20)   NOT NULL,
    gtid_set  TEXT,
    timestamp TIMESTAMP    NOT NULL,
    PRIMARY KEY (name)
);
```


## 3. Best effort clone(s)

"Offline" clones chunk up each table and copies it. Each chunk is consistent but since writes have not been stopped the chunks won't be consistent with each other.

```
cloner \
  clone \
  <...see below for config parameters...>
```

## 4. Consistent clone

Consistent clone starts off replicating from the source to the target and then starts snapshotting chunks in a similar way to best effort cloning in the step above. The difference is that it applies any replication changes to the chunk in memory from the point of read until it's written to the target. This makes the chunk strongly consistent. Since it's also replicating while the chunks aren't consistent with each other the database is kept in sync with the source during snapshotting which makes the entire process consistent.

```
cloner \
  replicate \
  --do-snapshot \
  <...see below for config parameters...>
```

(Omitting `--do-snapshot` will simply start replication without performing a clone.)

## 5. Wait for replication to catch up

Before we start checksumming or shifting we should make sure the target database isn't too far behind the source. Every 60 s (configurable) we write a heartbeat row to the source database which should get replicated to the target. The difference between the time of these heartbeats is the row. The minimum row resolution is the heartbeat frequency so it never drops entirely to zero.

There is a Prometheus metric called `replication_lag` or you can simply run the following query on both the source and the target and eye ball it:
```
SELECT * FROM _cloner_heartbeat\G
```

## 6. Checksumming

Checksumming compares all the cells of all the rows one chunk at the time. Since cloner does not stop replication a chunk could have differences. There are two reasons for a chunk difference: either there were writes in the chunk in between reading the chunk from the source and the target or there is an issue with the consistent clone. In order to differentiate between these two cases we simply retry the comparison a few times (using the `--failed-chunk-retry-count` parameter).

Unless the chunk is receiving extremely high write frequency we should eventually be able to read the chunk from the source and the target while there are no unreplicated writes.

A real issue with the consistent clone would never resolve itself regardless how many retries. In this case we should tear down the replication by clearing the checkpoint table and do another snapshot.

```
cloner \
  checksum \
  --failed-chunk-retry-count 10 \
  <...see below for config parameters...>
```

## 7. Shift traffic

Double check that replication lag isn't too high as in step 5 above.

Stop all writes to the source database. Easiest is to shut down the application or put it in maintenance mode. (If you have a read only mode in your application that is even better.)

Check that replication has caught up by waiting for the full amount of a heartbeat frequency (minimum 60 seconds) and then make sure there is no replication lag as in step 5 again.

Reconfigure your application to access the new target database and start it up again.

Congratulations you have now migrated!

(This traffic shifting approach uses restarts which results in quite high downtime. It's possible to use eg [SQLProxy](https://proxysql.com/) to decrease downtime even further.)

## Source and target config

The source and target datasources are configured in the same way with the following command line parameters:
```
--source-host=STRING                                         Hostname
--source-username=STRING                                     User
--source-password=STRING                                     Password
--source-database=STRING                                     Database name
--source-ca=STRING                                           CA root file, if this is specified then TLS will be enabled (PEM encoded)
--source-cert=STRING                                         Certificate file for client side authentication (PEM encoded)
--source-key=STRING                                          Key file for client side authentication (PEM encoded)
--target-host=STRING                                         Hostname
--target-username=STRING                                     User
--target-password=STRING                                     Password
--target-database=STRING                                     Database name
--target-ca=STRING                                           CA root file, if this is specified then TLS will be enabled (PEM encoded)
--target-cert=STRING                                         Certificate file for client side authentication (PEM encoded)
--target-key=STRING                                          Key file for client side authentication (PEM encoded)
```

There are many other command line options used to tune various parts of cloner, the documentation for those options are available by running `cloner <task> --help`.

They are also easy to read in the code: [clone and checksum options](https://github.com/cashapp/cloner/blob/main/pkg/clone/globals.go) and [additional options for replicate](https://github.com/cashapp/cloner/blob/main/pkg/clone/replicate.go#L94).

## Config file

By default cloner will clone all tables, if you want to list tables or specify further configuration for each table you can provide a config file with a TOML format like this:

```
[table."<table name>"]
ignore_columns = ["column_to_ignore"]

[table."<other table name>"]
```

Pass the name of the config file to the `-f` option.

There are many other table level options described here: https://github.com/cashapp/cloner/blob/main/pkg/clone/globals.go#L16

Of particular interest might be `target_where` and `source_where`. These options can be used to merge/split shards.

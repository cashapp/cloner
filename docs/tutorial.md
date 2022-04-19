# Tutorial: Migrating from one database to another

These are all long running operations and are best run as for example Kubernetes `job` for the finite jobs such as best effort clones and checksums or `deployment` for the replication/consistent clones.

## 1. Make sure your app works in the target database

This migration process is not reversible without some data loss. There is no replication from the target database to the source, so if the replication is reversed any writes that were sent to the target won't automatically be replicated back to the source. They can however be manually copied back.

For this reason it's a good idea to make sure your app runs well on your target database before you try the migration. For example:
* Make sure your CI tests run on the target database.
* After doing some best effort clonses (step 3 below) replay some shadow queries using for exampler [Chronicler](https://github.com/cashapp/chronicler).

## 2. Copy the schema

cloner does not currently copy the schema, this needs to be done by either running your migrations or simply dumping and copying. This is a good time to do any modifications you might need to the schema.

## 3. Best effort clone(s)

"Offline" clones chunk up each table and copies it. Each chunk is consistent but since writes have not been stopped the chunks won't be consistent with each other.

```
cloner \
  TODO args
```

## 4. Consistent clone

Consistent clone starts off replicating from the source to the target and then starts snapshotting chunks in a similar way to best effort cloning in the step above. The difference is that it applies any replication changes to the chunk in memory from the point of read until it's written to the target. This makes the chunk strongly consistent. Since it's also replicating while the chunks aren't consistent with each other the database is kept in sync with the source during snapshotting which makes the entire process consistent.

```
cloner \
  TODO args
```

## 5. Checksumming

Checksumming compares 


## 6. Shift traffic

Stop all writes to the source database. Easiest is to shut down the application or put it in maintenance mode. (If you have a read only mode in your application that is even better.)

Check that everything has fully replicated to the source by waiting for a full heartbeat (default 60 seconds) and then checking the replication lag is lower than the heartbeat frequency. There is a Prometheus metric called `replication_lag` or you can simply run the following query on both the source and the target and make sure it matches up:

```
SELECT TODO
```

Reconfigure your application to access the new target database and start it up again.

Congratulations you have now migrated!

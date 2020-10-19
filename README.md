Cloner clones one database into another. It reads both databases and diffs them and only writes the deltas with 
configurable parallelism. It will also eventually support faster "best effort" clones and slower "high fidelity" clones 
at a specific GTID.
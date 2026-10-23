# Counting data with DSBulk

DSBulk's `count` command counts rows in a table without writing the data to
any file. It can count globally, per host, per token range, or show the
largest partitions.

Counting is useful as a quick spot-check after a load, as a check before
a migration, or to find hot partitions.

The examples use a table named `iris_with_id` in a keyspace named
`dsbulkblog`. Replace these with your own values.

## The minimum count command

```text
dsbulk count -k dsbulkblog -t iris_with_id
```

DSBulk prints progress to stderr and then prints the total row count to
stdout:

```text
Operation directory: /tmp/logs/COUNT_20190314-171517-238903.

total | failed | rows/s | mb/s | kb/row | p50ms | p99ms | p999ms
  150 |      0 |    400 | 0.00 |   0.00 | 18.68 | 18.74 |  18.74

Operation COUNT_20190314-171517-238903 completed successfully in 0 seconds.

150
```

To print only the count and suppress the progress output, set verbosity to
`quiet`:

```text
dsbulk count -k dsbulkblog -t iris_with_id -verbosity quiet
```

Output:

```text
150
```

## Count modes

The `-stats` shortcut (`--stats.modes`) selects what to count. The default
is `global`. The following modes are available:

| Mode | What it counts |
| --- | --- |
| `global` | Total rows in the table |
| `hosts` | Rows per coordinator node |
| `ranges` | Rows per token range |
| `partitions` | Rows in the N largest partitions |

### global

The default. Counts all rows in the table:

```text
dsbulk count -k dsbulkblog -t iris_with_id -stats global
```

### hosts

Counts rows served by each coordinator:

```text
dsbulk count -k dsbulkblog -t iris_with_id \
  -verbosity quiet -stats hosts
```

Output (single-node cluster):

```text
node1/172.18.0.2:9042 150 100.00
```

Columns: host name and address, row count, percentage of total. The
host name part is empty when the address has no reverse name.

### ranges

Counts rows per token range:

```text
dsbulk count -k dsbulkblog -t iris_with_id \
  -verbosity quiet -stats ranges
```

Output (a node with vnodes, shortened):

```text
-8855590675907526914 -7236567380210483616 11 7.33
-7236567380210483616 -6258421927694369541 10 6.67
-6258421927694369541 -4479787626870782412 13 8.67
...
```

Columns: range start token, range end token, row count, percentage of
total. Each token range appears on its own line, so a cluster that uses
vnodes prints many lines.

### partitions

This mode reports the largest partitions by row count. It requires a table
that has a clustering column, because a table with only a partition key has
one row per partition.

Example setup:

```sql
CREATE TABLE dsbulkblog.iris_clustered (
  id INT,
  petal_length DOUBLE,
  petal_width DOUBLE,
  sepal_length DOUBLE,
  sepal_width DOUBLE,
  species TEXT,
  PRIMARY KEY ((species), id)
);
```

Load the data:

```text
dsbulk load -url /tmp/dsbulkblog/iris.csv \
  -k dsbulkblog -t iris_clustered
```

Count the largest partitions:

```text
dsbulk count -k dsbulkblog -t iris_clustered \
  -verbosity quiet -stats partitions
```

Output:

```text
'Iris-virginica'  50 33.33
'Iris-versicolor' 50 33.33
'Iris-setosa'     50 33.33
```

Columns: partition key value, row count, percentage of total.

By default, DSBulk reports the top 10 partitions. Change this with the
`-partitions` shortcut (`--stats.numPartitions`):

```text
dsbulk count -k dsbulkblog -t iris_clustered \
  -verbosity quiet -stats partitions -partitions 5
```

### Combining modes

Pass a list to count multiple groupings in a single run:

```text
dsbulk count -k dsbulkblog -t iris_with_id \
  -verbosity quiet --stats.modes "[ranges, hosts]"
```

Output:

```text
Total rows per node:
node1/172.18.0.2:9042 150 100.00
Total rows per token range:
-8855590675907526914 -7236567380210483616 11 7.33
-7236567380210483616 -6258421927694369541 10 6.67
...
```

Quote the list value or use the long form to avoid shell interpretation.

## Counting with a custom query

Use `-query` to count a filtered subset of rows:

```text
dsbulk count \
  -query "SELECT id FROM dsbulkblog.iris_with_id \
    WHERE petal_width = 2 ALLOW FILTERING"
```

DSBulk includes the full progress output. Add `-verbosity quiet` to
suppress it:

```text
6
```

A WHERE clause without token bounds runs as a single non-parallel query.
To parallelize, include the `:start` and `:end` token-range bounds:

```text
dsbulk count \
  -query "SELECT id FROM dsbulkblog.iris_with_id \
    WHERE petal_width = 2 \
    AND token(id) > :start AND token(id) <= :end \
    ALLOW FILTERING"
```

**Note:** when a custom query is provided, only `global` mode is supported.
Using `hosts`, `ranges`, or `partitions` with a custom query produces an
error.

## Using count as a spot-check

A common use after loading is to compare the row count in the source with
the count in Cassandra. If a load is expected to produce 1 000 000 unique
rows, a lower count may indicate key collisions or a partial load.

For migration workflows — unloading from one cluster and reloading into
another — see [cloud-and-astra.md — Migration](cloud-and-astra.md#migration)
for a complete example that uses a count to make sure the migration is
complete.

## Where to go next

- [loading.md](loading.md) — load data into a cluster.
- [unloading.md](unloading.md) — export data from a cluster.
- [cloud-and-astra.md](cloud-and-astra.md) — Astra DB, Amazon S3, and
  cluster-to-cluster migration.
- [../manual/settings.md](../manual/settings.md) — full option reference.

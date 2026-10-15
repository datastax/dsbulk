# Unloading data with DSBulk

This guide covers the `dsbulk unload` command: how to choose an output
location, select and reshape columns, export TTL and WRITETIME, and tune
parallel reads.

For connection options (host, port, authentication, TLS) see
[connection.md](connection.md). For loading, see [loading.md](loading.md).

The examples use a table named `iris_with_id` in a keyspace named
`dsbulkblog`. Replace these with your own values.

## The minimum unload command

```text
dsbulk unload -url /tmp/unload -k dsbulkblog -t iris_with_id
```

DSBulk prints progress and metrics to stderr and writes CSV files to
`/tmp/unload`. A typical progress line looks like:

```text
total | failed | rows/s |  p50ms |  p99ms | p999ms
  607 |      0 |    111 | 285.07 | 775.95 | 775.95
```

Some versions and connection types also show `mb/s` and `kb/row` columns.

At the end, DSBulk reports the operation ID and outcome:

```text
Operation UNLOAD_20190314-170354-717718 completed successfully in 0 seconds.
```

Suppress progress output by redirecting stderr to `/dev/null`:

```text
dsbulk unload -url /tmp/unload -k dsbulkblog -t iris_with_id 2>/dev/null
```

## Output location

### Directory

Pass a directory path with `-url`. DSBulk creates the directory if it does
not exist. DSBulk does not overwrite an existing non-empty directory. It
reports this error:

```text
Invalid value for connector.csv.url: target directory /tmp/unload must be empty.
```

### Standard output

The default value of `-url` is `-`, which means stdout. Omitting `-url`
sends all output to stdout, interleaved with progress on stderr:

```text
dsbulk unload -k dsbulkblog -t iris_with_id
```

Sample output:

```text
id,petal_length,petal_width,sepal_length,sepal_width,species
5,1.7,0.4,5.4,3.9,Iris-setosa
1,1.4,0.2,4.9,3.0,Iris-setosa
...
```

### File naming

When unloading to a directory, DSBulk creates one file per parallel thread.
The default pattern is `output-%06d.csv`, which produces names such as
`output-000001.csv`, `output-000002.csv`, and so on. Change the pattern with
`--connector.csv.fileNameFormat`:

```text
dsbulk unload -url /tmp/unload -k dsbulkblog -t iris_with_id \
  --connector.csv.fileNameFormat "iris-%06d.csv"
```

This produces `iris-000001.csv`, `iris-000002.csv`, etc. The format must be
a valid `String.format()` pattern containing `%d`.

When compression is enabled the default pattern already includes the
compression suffix. See [Compression](#compression).

### Number of output files

DSBulk determines the number of output files by the number of concurrent
writer threads. Control this with `-maxConcurrentFiles`
(`--connector.csv.maxConcurrentFiles`). The default, `AUTO`, lets DSBulk
choose based on available cores. Set it explicitly to cap file count:

```text
dsbulk unload -url /tmp/unload -k dsbulkblog -t iris_with_id \
  -maxConcurrentFiles 4
```

## CSV output options

### Delimiter

Change the field separator with `-delim`:

```text
dsbulk unload -k dsbulkblog -t iris_with_id -delim "\t"
```

### Null strings

Replace null values in the output with a placeholder string using
`-nullStrings`:

```text
dsbulk unload -k dsbulkblog -t iris_with_id -nullStrings "N/A"
```

Null cells appear as `N/A` in the output instead of an empty field.

### Max records per file

Limit the number of records written to each file with `-maxRecords`
(`--connector.csv.maxRecords`). When the limit is reached, DSBulk starts a
new file using the `fileNameFormat` pattern. Default is `-1` (no limit):

```text
dsbulk unload -url /tmp/unload -k dsbulkblog -t iris_with_id \
  --connector.csv.maxRecords 50000
```

## JSON output

To unload as JSON instead of CSV, specify `--connector.name json` and set a
`-url` that names a directory (for multiple files) or a single `.json` file:

```text
dsbulk unload -url /tmp/unload-json -k dsbulkblog -t iris_with_id \
  --connector.name json
```

The default file name pattern is `output-%06d.json`. Change it with
`--connector.json.fileNameFormat`.

Enable pretty printing with `--connector.json.prettyPrint true`:

```text
dsbulk unload -url /tmp/unload-json -k dsbulkblog -t iris_with_id \
  --connector.name json --connector.json.prettyPrint true
```

Pretty printing produces larger files. In testing with DSBulk 1.11.2 the
output was still one JSON object per line. Make sure the output is correct
before using this option.

The `-maxConcurrentFiles` and `-maxRecords` shortcuts also work for the JSON
connector.

## Compression

Compress output files by setting `--connector.csv.compression` (or
`--connector.json.compression` for JSON). The supported values are `gzip`,
`bzip2`, `lzma`, `xz`, `lz4`, `snappy`, `deflate`, and `zstd`. When
compression is enabled, the default `fileNameFormat` automatically includes
the appropriate suffix:

```text
dsbulk unload -url /tmp/unload -k dsbulkblog -t iris_with_id \
  --connector.csv.compression gzip
```

This produces files named `output-000001.csv.gz`, `output-000002.csv.gz`,
and so on.

## Choosing columns and rows

### Mapping on unload

By default, DSBulk unloads all columns. Use `-m` to select a subset:

```text
dsbulk unload -k dsbulkblog -t iris_with_id -m "id,species"
```

Output:

```text
id,species
23,Iris-setosa
114,Iris-virginica
53,Iris-versicolor
...
```

Use a function on the right-hand side of the mapping to add a computed
field. For example, to add a time-UUID generated by `now()` to each row:

```text
dsbulk unload -k dsbulkblog -t iris_with_id \
  -m "id=id, field1=now()"
```

The `now()` function generates a time-UUID value for each row.

### Custom SELECT query

Use `-query` to supply an explicit SELECT statement. A query that contains
no `WHERE`, `ORDER BY`, `GROUP BY`, or `LIMIT` clause is automatically split
into parallel token-range reads:

```text
dsbulk unload -query "SELECT id, species FROM dsbulkblog.iris_with_id"
```

With no clause, DSBulk injects:

```text
WHERE token(id) > :start AND token(id) <= :end
```

and runs one query per split, targeting coordinators that are also
replicas. Output is identical to the mapping example above.

### Parallel token-range query

If you write your own WHERE clause, include the `:start` and `:end` token
bounds to keep parallelism:

```text
dsbulk unload \
  -query "SELECT id, species FROM dsbulkblog.iris_with_id \
    WHERE token(id) > :start AND token(id) <= :end"
```

Bound variable names are arbitrary; only the pattern matters.

### Filtering rows

A WHERE clause without token bounds disables parallelism and runs as a
single query. Use this for small, targeted exports:

```text
dsbulk unload \
  -query "SELECT id, species FROM dsbulkblog.iris_with_id \
    WHERE id IN (101,102,103,104,105)"
```

Output:

```text
id,species
101,Iris-virginica
102,Iris-virginica
103,Iris-virginica
104,Iris-virginica
105,Iris-virginica
```

### Extra columns via custom query

To export system columns such as `writetime` or `ttl`, include them in the
SELECT list:

```text
dsbulk unload \
  -query "SELECT id, species, writetime(species) AS writetime \
    FROM dsbulkblog.iris_with_id"
```

Output:

```text
id,species,writetime
128,Iris-virginica,2026-10-14T03:32:51.219357Z
23,Iris-setosa,2026-10-14T03:32:51.219357Z
...
```

## Exporting TTL and WRITETIME

### With the preserve flags

The `-ttl` (`--schema.preserveTtl`) and `-timestamp`
(`--schema.preserveTimestamp`) flags tell DSBulk to include TTL and
WRITETIME columns for every non-primary-key column in the output. These
flags work on both load and unload. Each takes an explicit `true`:

```text
dsbulk unload -url /tmp/unload -k dsbulkblog -t iris_with_id \
  -ttl true -timestamp true
```

The output includes one extra column per non-key column for each flag. For
a column named `species`, DSBulk generates:

- `ttl(species)` — remaining TTL in seconds.
- `writetime(species)` — write timestamp, printed as an ISO-8601 instant.

DSBulk chooses these field names to match what the `-ttl` and
`-timestamp` flags produce when reloading the data. A round-trip export
and re-import preserves both values without any mapping changes.

DSBulk stops with an error when the flags are combined with `schema.query`.
They have no effect on counter tables.

### Renaming the generated fields

If you need different column headers, use a mapping entry with the `ttl()`
or `writetime()` function on the right-hand side. Write every entry in
`field=expression` form, because DSBulk rejects a mapping that mixes bare
column names with `field=expression` entries:

```text
dsbulk unload -k dsbulkblog -t iris_with_id \
  -m "id=id, species=species, species_ts=writetime(species)"
```

Output includes `id`, `species`, and `species_ts` instead of the default
`writetime(species)` header.

### Via a hand-written query

You can write the SELECT explicitly to produce the same result:

```text
dsbulk unload \
  -query "SELECT id, species, \
    writetime(species) AS writetime, ttl(species) AS ttl \
    FROM dsbulkblog.iris_with_id"
```

Do not combine a custom query with the `-ttl` and `-timestamp` flags:
DSBulk stops with an error.

## Read tuning

### Token-range splits

`--schema.splits` controls how many token-range segments DSBulk uses to
divide the full table read. The default is `8C` (8 × the number of
available CPU cores). Each split becomes one or more queries sent to the
coordinator that owns the range, which spreads the load across the cluster.

Increase splits if you see read timeouts:

```text
dsbulk unload -url /tmp/unload -k dsbulkblog -t iris_with_id \
  --schema.splits 16C
```

The `NC` syntax scales with core count. `--schema.splits` should always be
greater than `--engine.maxConcurrentQueries`. The actual number of splits
will never be less than the total number of primary token ranges in the
cluster, so the effective minimum is the ring size.

`--schema.splits` is used for unload and count; it is ignored during load.

### Concurrent queries

`-maxConcurrentQueries` (`--engine.maxConcurrentQueries`) limits how many
range queries run at the same time. The default is `AUTO`. Raise it (within
the splits budget) to saturate a fast cluster. Lower it to reduce read
pressure:

```text
dsbulk unload -url /tmp/unload -k dsbulkblog -t iris_with_id \
  --schema.splits 16C --engine.maxConcurrentQueries 8C
```

## Resume rules for unload

DSBulk supports checkpointing for unload operations just as it does for
load. When `--log.checkpoint.enabled` is `true` (the default), DSBulk
writes a `checkpoint.csv` file in the operation log directory.

**Important:** for unload, the input to the checkpoint is the set of token
ranges and the query used to read them. To resume correctly, the following
must be identical to the original run:

- the `-query` or the keyspace and table (`-k`, `-t`).
- the number of splits (`--schema.splits`).
- the token distribution of the cluster (no topology changes between runs).

Pass the checkpoint path from the first run to the second:

```text
dsbulk unload -url /tmp/unload -k dsbulkblog -t iris_with_id \
  --log.checkpoint.file ./logs/UNLOAD_.../checkpoint.csv
```

If any of the above conditions have changed, do not resume — run a fresh
unload instead to avoid inconsistent output.

For more on checkpoint options see [loading.md — Checkpoint and
resume](loading.md#checkpoint-and-resume).

## Piping output to other tools

Because unload defaults to stdout when `-url` is omitted, you can pipe the
CSV output directly into shell tools. Redirect stderr to suppress the
progress output:

```text
dsbulk unload -k dsbulkblog -t iris_with_id 2>/dev/null \
  | sed 's/Iris-//g' > /tmp/iris_shortname.csv
```

First few lines of the result:

```text
id,petal_length,petal_width,sepal_length,sepal_width,species
144,5.7,2.5,6.7,3.3,virginica
23,1.7,0.5,5.1,3.3,setosa
114,5.1,2.4,5.8,2.8,virginica
```

For migrating data between clusters through a pipe, see
[cloud-and-astra.md — Migration](cloud-and-astra.md#migration).

## Date and codec formats

Use `--codec.date` to control how date columns are formatted in the output:

```text
dsbulk unload -k dsbulkblog -t president_birthdates \
  -delim "\t" --codec.date "EEEE MMMM d, y GGGG"
```

The same `--codec.time`, `--codec.timestamp`, `--codec.timezone`, and
locale options available during load apply equally to unload. See
[loading.md — Formats and locale](loading.md#formats-and-locale).

## Where to go next

- [counting.md](counting.md) — count rows or inspect partition sizes.
- [cloud-and-astra.md](cloud-and-astra.md) — Astra DB, Amazon S3, and
  cluster-to-cluster migration.
- [connection.md](connection.md) — host, port, authentication, and TLS.
- [../manual/settings.md](../manual/settings.md) — full option reference.

# Loading data with DSBulk

This guide covers how to load data into a Cassandra cluster using the `load`
subcommand. It is based on the original blog series (Parts 1 and 2) and
extended with options confirmed from the DSBulk codebase.

For connection options (host, port, authentication, TLS) see
[connection.md](connection.md). For loading from Astra DB or Amazon S3, see
[cloud-and-astra.md](cloud-and-astra.md). The complete option reference is in
[manual/settings.md](../manual/settings.md).

## The minimum load command

The three required options are `-url`, `-k`, and `-t`:

```text
dsbulk load -url /tmp/dsbulkblog/iris.csv \
  -k dsbulkblog -t iris_with_id
```

- `-url` — path to the file, directory, or URL to read from.
- `-k` — keyspace name.
- `-t` — table name.

DSBulk reads the first line as a CSV header and maps field names to column
names automatically. The output shows a stats line followed by a completion
message:

```text
total | failed | rows/s | mb/s | kb/row | p50ms | p99ms | p999ms | batches
  150 |      0 |    329 | 0.02 |   0.05 |  9.84 | 39.58 |  45.88 |    1.00

Operation LOAD_20190314-161940-640593 completed successfully in 0 seconds.
```

The `total` column is the number of records sent; `failed` is how many were
rejected. The remaining columns are throughput and latency percentiles.

Each run creates a log directory under `./logs/` (configurable with
`-logDir`). The name encodes the operation type and timestamp, for example
`LOAD_20190314-161940-640593`.

## Where input comes from

### A single file, directory, or HTTP URL

All of these forms are valid for `-url`:

```text
dsbulk load -url /tmp/dsbulkblog/iris.csv   -k dsbulkblog -t iris_with_id
dsbulk load -url /tmp/dsbulkblog/iris/       -k dsbulkblog -t iris_with_id
dsbulk load -url https://example.com/iris.csv -k dsbulkblog -t iris_with_id
```

When given a directory, DSBulk reads every file that matches the
`fileNamePattern` (default `**/*.csv` for the CSV connector). To restrict
the files, pass the long option:

```text
dsbulk load -url /tmp/dsbulkblog/iris -k dsbulkblog -t iris_with_id \
  --connector.csv.fileNamePattern "**/*.csv"
```

Set `--connector.csv.recursive` to `true` to descend into subdirectories
(default `false`).

### Standard input

Omitting `-url`, or passing `-url -`, reads from stdin. You can pipe the
output of any command-line tool into DSBulk:

```text
cat /tmp/dsbulkblog/iris.csv | sed 's/Iris-//g' | \
  dsbulk load -k dsbulkblog -t iris_with_id
```

```text
cat /tmp/dsbulkblog/iris_no_header.csv | \
  awk 'BEGIN{print "sepal_length,sepal_width,petal_length,petal_width,species,id"} {print $0}' | \
  dsbulk load -k dsbulkblog -t iris_with_id
```

### A list of URLs (urlfile)

To load from many URLs, write them one per line in a text file and pass
that file with `--connector.csv.urlfile`:

```text
dsbulk load -k dsbulkblog -t iris_with_id \
  --connector.csv.urlfile /tmp/urls.txt
```

### Amazon S3

Pass an `s3://` URL as `-url`. See [cloud-and-astra.md](cloud-and-astra.md)
for the required options.

## CSV options

### Delimiter

The default delimiter is a comma. Change it with `-delim`:

```text
dsbulk load -url /tmp/dsbulkblog/president_birthdates.psv \
  -k dsbulkblog -t president_birthdates -delim "|"
```

`-delim` is a shortcut for `--connector.csv.delimiter`.

### Header

By default, DSBulk treats the first row as a header that names the fields.
To skip it and supply the mapping yourself, pass `-header false`.

### Comment character

To ignore lines that start with a comment character, use `-comment`:

```text
dsbulk load -url /tmp/dsbulkblog/iris_with_comment.csv \
  -k dsbulkblog -t iris_with_id -comment "#"
```

### Whitespace

By default, DSBulk keeps leading and trailing whitespace inside CSV values.
To strip it, set these options:

```text
dsbulk load -url /tmp/dsbulkblog/iris.csv -k dsbulkblog -t iris_with_id \
  --connector.csv.ignoreLeadingWhitespaces true \
  --connector.csv.ignoreTrailingWhitespaces true
```

### Null and empty values

An empty field in the input maps to a Cassandra `NULL` by default. To
replace it with a non-null sentinel, use `--connector.csv.nullValue`:

```text
dsbulk load -url /tmp/dsbulkblog/iris_with_nulls.csv \
  -k dsbulkblog -t iris_with_id --connector.csv.nullValue "IRIS"
```

### Large fields and wide tables

Two limits protect against excessively large inputs. Raise them when
needed:

- `--connector.csv.maxCharsPerColumn` (default 4096) — maximum characters
  per field.
- `--connector.csv.maxColumns` (default 512) — maximum columns per row.

## JSON connector

Use `-c json` to switch to the JSON connector:

```text
dsbulk load -c json -url /tmp/data/ -k dsbulkblog -t iris_with_id
```

The connector defaults to `MULTI_DOCUMENT` mode, where each line is a
complete JSON object. For a file containing a single root-level JSON
array, set `--connector.json.mode SINGLE_DOCUMENT`.

The `fileNamePattern` defaults to `**/*.json`. Options such as `-url`,
`-encoding`, `-skipRecords`, `-maxRecords`, and `-maxConcurrentFiles` work
the same as for CSV. There is no `-delim`, `-header`, or `-comment` for
JSON.

## Compression

Set `--connector.csv.compression` (or `--connector.json.compression`) to
one of the supported codecs. There is no automatic detection from the file
extension.

Read and write codecs: `none` (default), `xz`, `gzip`, `bzip2`, `zstd`,
`lz4`, `lzma`, `snappy`, `deflate`.

Read-only codecs: `brotli`, `z`, `deflate64`.

```text
dsbulk load -url /tmp/data/iris.csv.gz -k dsbulkblog -t iris_with_id \
  --connector.csv.compression gzip
```

## Mapping fields to columns

### Automatic mapping (like-named)

When the header field names match the table column names exactly, no `-m`
option is needed. This is the default.

To state it explicitly: `-m "*=*"`.

### Named mapping

Map each input field to the column that should receive it:

```text
dsbulk load -url /tmp/dsbulkblog/iris.csv -k dsbulkblog -t iris_with_id \
  -m "sepal_l=sepal_length, sepal_w=sepal_width, petal_l=petal_length, \
      petal_w=petal_width, species_name=species, id_number=id"
```

### Like-named plus one rename

The wildcard and an explicit override can be combined:

```text
dsbulk load -url /tmp/dsbulkblog/iris.csv -k dsbulkblog -t iris_with_id \
  -m "* = *, species_name = species"
```

### Ordered list

Provide the column names in the order the fields appear in the input:

```text
dsbulk load -url /tmp/dsbulkblog/iris.csv -k dsbulkblog -t iris_with_id \
  -m "sepal_length,sepal_width,petal_length,petal_width,species,id"
```

### Mapping by index (no header)

Use zero-based column numbers when the file has no header:

```text
dsbulk load -url /tmp/dsbulkblog/iris_no_header.csv \
  -k dsbulkblog -t iris_with_id -header false \
  -m "0=sepal_length,1=sepal_width,2=petal_length,3=petal_width,4=species,5=id"
```

### Excluding fields

To load all like-named fields except one:

```text
dsbulk load -url /tmp/dsbulkblog/iris.csv -k dsbulkblog -t iris_with_id \
  -m "* = -species"
```

To exclude more than one field:

```text
dsbulk load -url /tmp/dsbulkblog/iris.csv -k dsbulkblog -t iris_with_id \
  -m "* = [-species, -petal_length]"
```

### CQL function as the input side

Use a CQL function to generate a value when no source field exists. For
example, to populate `c4` with the current timestamp:

```text
dsbulk load -url /tmp/data.csv -k mykeyspace -t mytable \
  -m "* = *, now() = c4"
```

### Extra and missing fields

By default, DSBulk silently ignores extra input fields, and missing non-key
fields cause an error. Change these defaults with:

- `--schema.allowExtraFields` (default `true`) — set `false` to reject rows
  with unmapped fields.
- `--schema.allowMissingFields` (default `false`) — set `true` to allow
  rows that omit non-key columns.

```text
dsbulk load -url /tmp/data.csv -k dsbulkblog -t iris_with_id \
  --schema.allowMissingFields true
```

## Null handling

### Null strings

A field that contains an explicit string such as `"NULL"` is not treated as
a Cassandra null by default. Use `-nullStrings` to specify which string
values should be converted to null:

```text
dsbulk load -url /tmp/dsbulkblog/iris_with_null_string.csv \
  -k dsbulkblog -t iris_with_id -nullStrings "NULL"
```

### nullToUnset

By default, null input values are written to Cassandra as `UNSET`, not as
`NULL`. This avoids creating tombstones for columns that are simply absent
from the source. To write nulls explicitly, set `--schema.nullToUnset
false`.

## Formats and locale

Use `--codec.date` to specify the date format when the default
(`yyyy-MM-dd`) does not match your data. The format string follows
Java's `DateTimeFormatter` pattern syntax:

```text
dsbulk load -url /tmp/logs/LOAD_.../mapping.bad \
  -k dsbulkblog -t president_birthdates \
  -delim "|" -header false -m "president,birthdate_string,birthdate" \
  --codec.date "y.M.d"
```

Related `codec.*` settings:

- `--codec.time` — time format (default `HH:mm:ss`).
- `--codec.timestamp` — timestamp format.
- `-locale` (`--codec.locale`) — locale for parsing numbers and dates.
- `-timeZone` (`--codec.timeZone`) — time zone for timestamps without an
  explicit offset.

## Collections and UDTs

Collections and UDT values are expressed as JSON inside the CSV field.
The field must be quoted, and all string elements inside the JSON must be
escaped:

```text
category,examples
"Baseball Teams","[\"Boston Red Sox\",\"Atlanta Braves\"]"
```

Load this as normal — no special option is needed:

```text
dsbulk load -url /tmp/dsbulkblog/sportsteams.csv \
  -k dsbulkblog -t categories_list
```

One caveat: DSBulk **replaces** the existing collection. It does not append
to the existing collection. Make sure there is no leading space between the
delimiter and the opening quote, or set
`--connector.csv.ignoreLeadingWhitespaces true`.

## Custom queries

### INSERT with named bindings

Provide the full INSERT statement with `-query`. The `-k` and `-t` options
are not needed when the keyspace and table are in the query:

```text
dsbulk load -url /tmp/dsbulkblog/iris.csv \
  -query "INSERT INTO dsbulkblog.iris_with_id(id,petal_width,petal_length, \
      sepal_width,sepal_length,species) \
    VALUES (:id,:petal_width,:petal_length,:sepal_width,:sepal_length,:species)"
```

### INSERT with positional bindings

Use `?` placeholders. Column order must match the input field order:

```text
dsbulk load -url /tmp/dsbulkblog/iris_no_header.csv \
  -query "INSERT INTO dsbulkblog.iris_with_id \
      (petal_width,petal_length,sepal_width,sepal_length,species,id) \
    VALUES (?,?,?,?,?,?)" \
  -header false
```

### Constant values in a query

Embed a literal in the query to set the same value for all rows:

```text
dsbulk load -url /tmp/dsbulkblog/iris.csv \
  -query "INSERT INTO dsbulkblog.iris_with_id \
      (id,petal_width,petal_length,sepal_width,sepal_length,species) \
    VALUES (:id,:petal_width,:petal_length,:sepal_width,:sepal_length, \
      'some kind of iris')"
```

### DELETE via load

Pass a DELETE statement to remove rows identified in the input file:

```text
head -11 /tmp/dsbulkblog/iris.csv | \
  dsbulk load -query "DELETE FROM dsbulkblog.iris_with_id WHERE id=:id"
```

### CQL functions in queries

Call a CQL function directly in the INSERT:

```text
dsbulk load -url /tmp/dsbulkblog/president_birthdates.psv \
  -delim "|" \
  -query "INSERT INTO dsbulkblog.president_birthdates \
      (president,birthdate,birthdate_string) \
    VALUES (:president, CurrentDate(), :birthdate_string)"
```

Or use the `-m` shorthand:

```text
dsbulk load -url /tmp/dsbulkblog/president_birthdates.psv \
  -k dsbulkblog -t president_birthdates -delim "|" \
  -m "president=president, currentdate()=birthdate, \
      birthdate_string=birthdate_string"
```

In a mapping, write the function name in lower case, as in `currentdate()`.
Mapping identifiers are case-sensitive, so `CurrentDate()` fails with
`Unknown function CurrentDate called`. Inside a `-query` the CQL parser
handles the name, so either case works there.

## TTL and timestamp

### Constant TTL or timestamp

Apply the same TTL (in seconds) to every row with `--schema.queryTtl`:

```text
dsbulk load -url /tmp/dsbulkblog/iris.csv \
  -k dsbulkblog -t iris_with_id --schema.queryTtl 3600
```

Apply the same write timestamp with `--schema.queryTimestamp`:

```text
dsbulk load -url /tmp/dsbulkblog/iris.csv \
  -k dsbulkblog -t iris_with_id \
  --schema.queryTimestamp "2018-01-01T12:34:56+00:00"
```

### Per-row TTL from the input

Use the `__ttl` pseudo-column in the mapping to take the TTL from a field
in each row:

```text
cat /tmp/dsbulkblog/iris_no_header.csv | \
  awk -F, '{printf("%s,%d\n", $0, $6+10000);}' | \
  dsbulk load -k dsbulkblog -t iris_with_id -header false \
    -m "0=sepal_length,1=sepal_width,2=petal_length,3=petal_width, \
        4=species,5=id,6=__ttl"
```

Use indexed entries (`6=__ttl`) when the input has no header. A plain
ordered list that ends in `__ttl` fails with `simple entries cannot contain
function calls`. With a header row, name the field instead, for example
`-m "* = *, ttl = __ttl"`.

To bind the TTL in a custom query, use `USING TTL :colname`:

```text
cat /tmp/dsbulkblog/iris_no_header.csv | \
  awk -F, 'BEGIN{printf("sepal_length,sepal_width,petal_length,petal_width,species,id,ttl_to_use\n");} \
    {printf("%s,%d\n", $0, $6+10000);}' | \
  dsbulk load \
    -query "INSERT INTO dsbulkblog.iris_with_id \
        (sepal_length,sepal_width,petal_length,petal_width,species,id) \
      VALUES (:sepal_length,:sepal_width,:petal_length,:petal_width,:species,:id) \
      USING TTL :ttl_to_use"
```

### Preserving existing TTL and WRITETIME

To preserve the TTL and write-time values from the source cluster when
migrating, use the `-ttl` and `-timestamp` shortcuts
(`schema.preserveTtl` and `schema.preserveTimestamp`), each with an
explicit `true`. These generate a BATCH statement internally. DSBulk
stops with an error when they are combined with a custom `schema.query`.
They have no effect on counter tables.

See [cloud-and-astra.md](cloud-and-astra.md#migration) for a complete
migration example.

## Skipping and limiting records

Start reading after a given number of rows with `-skipRecords`:

```text
dsbulk load -url /tmp/dsbulkblog/iris.csv \
  -k dsbulkblog -t iris_with_id -skipRecords 100
```

Load at most a given number of records with `-maxRecords`:

```text
dsbulk load -url /tmp/dsbulkblog/iris.csv \
  -k dsbulkblog -t iris_with_id -maxRecords 20
```

Combine them to load a slice:

```text
dsbulk load -url /tmp/dsbulkblog/iris.csv \
  -k dsbulkblog -t iris_with_id -skipRecords 100 -maxRecords 20
```

## Errors and logs

### Log directory

Logs are written to `./logs/` by default. Change the location with
`-logDir`:

```text
dsbulk load -url /tmp/dsbulkblog/iris.csv \
  -k dsbulkblog -t iris_with_id -logDir /var/log/dsbulk
```

Each run creates a subdirectory named `LOAD_<timestamp>`.

### Error threshold

DSBulk stops when the number of failed records reaches `--log.maxErrors`
(default 100). To set a percentage threshold instead:

```text
dsbulk load -url /tmp/data.csv -k mykeyspace -t mytable \
  --log.maxErrors "20%"
```

### Verbosity

Control the detail of console output with `-verbosity`:

```text
dsbulk load -url /tmp/dsbulkblog/iris.csv \
  -k dsbulkblog -t iris_with_id -verbosity quiet
```

Valid values are `quiet`, `normal` (default), `high`, and `max`.
Setting `quiet` disables the progress line entirely.

### Bad files and rejected records

When a row fails to parse or to be converted, DSBulk writes the raw input
line to `mapping.bad` in the log directory. You can re-run DSBulk on that
file after correcting the mapping:

```text
dsbulk load -url /tmp/logs/LOAD_.../mapping.bad \
  -k dsbulkblog -t iris_with_id \
  -header false -m "0=sepal_length,1=sepal_width,2=petal_length, \
    3=petal_width,4=species,5=id"
```

A companion `mapping-errors.log` contains the full exception for each
rejected row.

## Checkpoint and resume

DSBulk writes a `checkpoint.csv` file inside each operation log directory
when `--log.checkpoint.enabled` is `true` (the default). The exact path is
printed at the end of the run:

```text
--dsbulk.log.checkpoint.file=./logs/LOAD_.../checkpoint.csv
```

To resume a previous run, pass that path back as
`--log.checkpoint.file`:

```text
dsbulk load -url /tmp/dsbulkblog/iris.csv \
  -k dsbulkblog -t iris_with_id \
  --log.checkpoint.file ./logs/LOAD_.../checkpoint.csv
```

The `--log.checkpoint.replayStrategy` controls which records are retried:

- `retry` (default) — process new and rejected records from resources that
  were not consumed entirely. Rows may be inserted twice, so use it only
  when the load is idempotent.
- `resume` — process only new records from resources that were not consumed
  entirely. Records already processed are skipped, including rejected ones.
  This is the safest choice when the load is not idempotent. If there is
  nothing left to process, DSBulk stops with `Nothing to replay using replay
  strategy resume`.
- `retryAll` — like `retry`, but also re-processes resources that were
  consumed entirely. Use it only when the load is idempotent.

**Important:** do not move or rename the input files between runs. DSBulk
uses the file paths stored in the checkpoint to match records.

Checkpoint uses a small amount of extra memory and adds a slight overhead.
Disable it for performance-critical single-shot loads:

```text
dsbulk load -url /tmp/dsbulkblog/iris.csv \
  -k dsbulkblog -t iris_with_id --log.checkpoint.enabled false
```

## Where to go next

- [tuning-and-monitoring.md](tuning-and-monitoring.md) — throughput
  limits, batching, dry run, and monitoring.
- [connection.md](connection.md) — host, port, authentication, TLS.
- [unloading.md](unloading.md) — export data from a cluster.
- [counting.md](counting.md) — count rows and report partition statistics.
- [cloud-and-astra.md](cloud-and-astra.md) — Astra DB, Amazon S3, and
  cross-cluster migration.
- [manual/settings.md](../manual/settings.md) — every option with its
  default and full description.

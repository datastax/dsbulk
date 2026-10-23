# Cloud storage, Astra DB, and cluster migration

This guide covers three situations where the data source or destination is
not a local file or a directly connected Cassandra cluster:

- **Astra DB**: connecting with a secure connect bundle.
- **Other storage**: reading from Amazon S3 with the built-in `s3://` URL
  handler, and handling the lack of native support for Google Cloud
  Storage and Azure Blob Storage.
- **Other systems**: migrating data between clusters or tables, with
  options to preserve TTL and WRITETIME, and loading from other systems
  through a pipe.

The examples use a table named `iris_with_id` in a keyspace named
`dsbulkblog`. Replace these with your own values.

## Astra DB with a Secure Connect Bundle

To connect to DataStax Astra DB, download the secure connect bundle for your
database and pass its location with the `-b` shortcut
(`--datastax-java-driver.basic.cloud.secure-connect-bundle`):

```text
dsbulk load -b /path/to/secure-connect-mydb.zip \
  -u my-client-id \
  -p my-client-secret \
  -k dsbulkblog -t iris_with_id \
  -url /tmp/iris.csv
```

### Accepted path forms for `-b`

The bundle path can be any of:

- An absolute path: `/path/to/bundle.zip`
- A path relative to the working directory: `./bundle.zip`
- A path relative to the home directory: `~/bundle.zip`
- A Windows path: `C:\\path\\to\\bundle.zip` (double-backslash in HOCON)
- A `file:` URL: `file:/path/to/bundle.zip`
- An `http:` URL: `http://host.example.com/bundle.zip`

### What is ignored with a bundle

When a bundle is present, DSBulk assumes you are connecting to Astra DB.
DSBulk ignores settings that are not compatible with cloud deployments:

- Explicit contact points (`-h`) are ignored, with a warning.
- The consistency level (`-cl`) and the SSL/TLS settings
  (`--datastax-java-driver.advanced.ssl-engine-factory.*`) are ignored,
  with a log message.

For the full list, see the `-b` entry in
[../manual/settings.md](../manual/settings.md).

### Credentials

Pass the username with `-u` and the password with `-p`, as you would for
any other cluster that requires authentication.

## Amazon S3

DSBulk includes a built-in URL handler for `s3://` URLs. S3 access is
**read-only**. You can load from S3 but cannot unload to it.

### URL format

```text
s3://bucket-name/path/to/object.csv?region=us-east-1&profile=my-profile
```

The query string is always required. Leaving it blank causes an error.
The `region` parameter is required. All other parameters are optional.

### S3 credentials

DSBulk supports three credential strategies, tried in priority order:

1. **AWS named profile** (`profile=<name>`). Reads from the AWS credentials
   file. This method is preferred because credentials stay out of the URL:

   ```text
   dsbulk load -k dsbulkblog -t iris_with_id \
     -url "s3://my-bucket/data.csv?region=eu-west-1&profile=my-profile"
   ```

2. **Inline access keys** (`accessKeyId=<id>&secretAccessKey=<secret>`).
   DSBulk logs a warning that this is insecure. DSBulk requires both keys
   when you supply either one:

   ```text
   dsbulk load -k dsbulkblog -t iris_with_id \
     -url "s3://my-bucket/data.csv?region=eu-west-1&accessKeyId=<id>&secretAccessKey=<secret>"
   ```

3. **Default AWS credentials chain**. When neither `profile` nor key
   parameters are present, DSBulk uses the standard AWS credential
   chain (environment variables, instance metadata, etc.):

   ```text
   dsbulk load -k dsbulkblog -t iris_with_id \
     -url "s3://my-bucket/data.csv?region=eu-west-1"
   ```

### Client cache

DSBulk caches S3 clients to avoid rebuilding a client for every file when
loading from a directory of S3 URLs. The default cache size is 20 and can
be changed with `--dsbulk.s3.clientCacheSize`. In most cases the default is
sufficient.

## Google Cloud Storage and Azure Blob Storage

GCS and Azure are not natively supported. Use one of these workarounds:

1. **Mounted filesystem**. Mount the GCS bucket (with `gcsfuse`) or the
   Azure container (with `blobfuse`) as a local directory, then pass the
   mount path as `-url`:

   ```text
   dsbulk load -k dsbulkblog -t iris_with_id \
     -url /mnt/gcs-bucket/data.csv
   ```

2. **Pre-signed or SAS HTTPS URL**. Generate a time-limited HTTPS URL in
   the cloud console or CLI and pass it directly:

   ```text
   dsbulk load -k dsbulkblog -t iris_with_id \
     -url "https://storage.googleapis.com/bucket/data.csv?X-Goog-Signature=..."
   ```

3. **Piped cloud CLI**. Pipe the output of the provider's command-line tool
   into DSBulk on standard input. For example, with `gsutil`:

   ```text
   gsutil cat gs://my-bucket/data.csv | dsbulk load \
     -k dsbulkblog -t iris_with_id
   ```

   The default `-url` value is `-`, which reads standard input. One thread
   reads a single stream.

## Migration

DSBulk has no dedicated migrate command. Migration is a two-step process:
unload from the source, then load into the destination.

### Through the filesystem

Unload to a local directory, then load from it:

```text
dsbulk unload -h source-host -k dsbulkblog -t iris_with_id \
  -url /tmp/migrate

dsbulk load -h dest-host -k dsbulkblog -t iris_with_id \
  -url /tmp/migrate
```

The unload finishes completely before the load begins. Both operations run
with their normal concurrency settings.

### Through a pipe

You can connect the two in a single command:

```text
dsbulk unload -h source-host -k dsbulkblog -t iris_with_id | \
  dsbulk load -h dest-host -k dsbulkblog -t iris_with_id
```

**Note:** piping forces both sides to use a single thread for I/O because
data flows through stdout/stdin. For large tables, the two-step approach
through the filesystem is faster.

### Preserving TTL and WRITETIME

To carry TTL and write-time values across clusters, use the `-ttl` and
`-timestamp` shortcuts on both commands. Both take an explicit `true`.

**Unload:**

```text
dsbulk unload -h source-host -k dsbulkblog -t iris_with_id \
  -url /tmp/migrate \
  -ttl true -timestamp true
```

DSBulk generates `ttl(<col>)` and `writetime(<col>)` field names in the
output for every non-primary-key column.

**Load:**

```text
dsbulk load -h dest-host -k dsbulkblog -t iris_with_id \
  -url /tmp/migrate \
  -ttl true -timestamp true
```

DSBulk generates a BATCH statement internally to set individual TTL and
WRITETIME values per row. The field names produced by the unload step are
matched automatically.

These flags require DSBulk to auto-generate the CQL query. DSBulk does not
support them with an explicit `-query` argument or with counter tables. See
[loading.md — Preserving existing TTL and WRITETIME](loading.md#preserving-existing-ttl-and-writetime)
and
[unloading.md — Exporting TTL and WRITETIME](unloading.md#exporting-ttl-and-writetime)
for details.

### Alternative: manual BATCH with a custom query

For tables where the shortcut flags are insufficient (for example, when
you need to rename TTL fields or target a subset of columns), you can write
the queries manually:

**Unload with explicit writetime and TTL aliases:**

```text
dsbulk unload -h source-host \
  -query "SELECT id,
    petal_length, WRITETIME(petal_length) AS w_petal_length,
    TTL(petal_length) AS l_petal_length,
    petal_width, WRITETIME(petal_width) AS w_petal_width,
    TTL(petal_width) AS l_petal_width
    FROM dsbulkblog.iris_with_id" \
  -url /tmp/migrate
```

**Load with a BATCH query:**

```text
dsbulk load -h dest-host \
  -query "BEGIN BATCH
    INSERT INTO dsbulkblog.iris_with_id(id, petal_length)
      VALUES (:id, :petal_length)
      USING TIMESTAMP :w_petal_length AND TTL :l_petal_length;
    INSERT INTO dsbulkblog.iris_with_id(id, petal_width)
      VALUES (:id, :petal_width)
      USING TIMESTAMP :w_petal_width AND TTL :l_petal_width;
    APPLY BATCH;" \
  --batch.mode DISABLED \
  -url /tmp/migrate
```

`--batch.mode DISABLED` prevents DSBulk's batching logic from
conflicting with the BATCH statement you have written explicitly.

### Copying a table within one cluster to change its primary key

CQL does not allow changing partition keys or clustering columns after a
table is created. To migrate to a new schema, create the new table and then
pipe the data into it:

```text
cqlsh -e "CREATE TABLE dsbulkblog.iris_by_species(
  id INT,
  petal_length DOUBLE,
  petal_width DOUBLE,
  sepal_length DOUBLE,
  sepal_width DOUBLE,
  species TEXT,
  PRIMARY KEY ((species), id));"

dsbulk unload -k dsbulkblog -t iris_with_id | \
  dsbulk load -k dsbulkblog -t iris_by_species
```

Run a spot-check count on both tables to make sure the migration is complete:

```text
dsbulk count -k dsbulkblog -t iris_with_id \
  -stats global --log.verbosity quiet
dsbulk count -k dsbulkblog -t iris_by_species \
  -stats global --log.verbosity quiet
```

Both counts should match. See [counting.md](counting.md) for more on the
`count` command.

## Loading from other systems through a pipe

Any command-line tool that writes CSV to standard output can feed DSBulk
through a pipe.

### MySQL

MySQL's `mysql` client can export a query result in tab-delimited form
with `-B`. Pipe that directly into DSBulk:

```text
mysql \
  --user=mysqluser \
  --password=mypassword \
  -B \
  --execute="SELECT id, petal_length, petal_width,
    sepal_length, sepal_width, species
    FROM mydb.iris_with_id" | \
dsbulk load -k dsbulkblog -t iris_with_id \
  -delim "\t" \
  -header false \
  -m "id, petal_length, petal_width, sepal_length, sepal_width, species"
```

`-delim "\t"` tells DSBulk to expect tab delimiters. `-header false` is
required because MySQL does not include a CSV header row in `-B` output.
Supply an explicit mapping (`-m`) so that columns are matched in the right
order.

### DSEFS (DSE only)

DSE Analytics clusters include DSEFS (DataStax Enterprise File System).
DSBulk has no native DSEFS URL handler, but you can pipe data out using
the `dse fs` command:

```text
dse fs "cat dsefs:///dsbulkblog/iris.csv" | \
  dsbulk load -k dsbulkblog -t iris_with_id
```

Alternatively, if the DSEFS WebHDFS interface is enabled, you can pass the
WebHDFS URL directly as `-url`:

```text
dsbulk load -k dsbulkblog -t iris_with_id \
  -url "http://localhost:5598/webhdfs/v1/dsbulkblog/iris.csv?op=OPEN"
```

## Where to go next

- [loading.md](loading.md) — full loading guide including CSV, JSON,
  mapping, TTL, throughput tuning, and checkpointing.
- [unloading.md](unloading.md) — full unloading guide including output
  format, column selection, splits, and TTL export.
- [counting.md](counting.md) — counting rows by mode, including a
  post-migration spot-check example.
- [connection.md](connection.md) — cluster connectivity, authentication,
  SSL/TLS, and configuration files.
- [../manual/settings.md](../manual/settings.md) — complete option
  reference including every shortcut and default value.

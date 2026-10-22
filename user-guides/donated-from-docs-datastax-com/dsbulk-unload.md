# Unload data

Use the `dsbulk unload` command to extract data from a database table into a CSV or JSON file:

```bash
dsbulk unload \
  -url DESTINATION \
  -k KEYSPACE_NAME -t TABLE_NAME \
  OPTIONS
```

Replace the following:

- **`DESTINATION`**: The location where you want to write the extracted data.

  When unloaded to a CSV file, column names in a table map to column names in the CSV data.

  When unloaded to a JSON file, column names in a table map to field names in the JSON data.

  Field names must be emitted in a _header row_ in the output.

- **`KEYSPACE_NAME`** and **`TABLE_NAME`**: The name of the keyspace (`-k`, `--schema.keyspace`) and table (`-t`, `--schema.table`) from which you want to unload the data.

  Or, use other [schema options](options/schema-options.md) to specify the target table, such as `-query` to select a table with a Cassandra Query Language (CQL) statement or `-g` to unload graph data.

- **`OPTIONS`**: Any options for the `unload` command, authentication options, or other [DSBulk options](02-usage.md).

> [!TIP]
> To avoid unloading excessive amounts of tombstones from DSE, HCD, or open-source Cassandra clusters, it is recommended that you run garbage collection (GC) _at the cell level_ before you run `dsbulk unload`.
> For example, you can run GC with `nodetool garabagecollect` or `nodetool compact -s`.

## Unload locally without authentication

The following command unloads data to `stdout` from a local cluster (`localhost` [contact point](options/driver-options.md)) without authentication:

```bash
dsbulk unload -k ks1 -t table1
```

## Unload with authentication

Pass the relevant options with your `unload` commands if your cluster requires authentication or uses SSL encryption.

You can pass the values directly or [use a configuration file](02-usage.md#use-a-configuration-file).

### Authentication for self-managed clusters (DSE, HCD, and Cassandra)

If your database requires authentication, provide the username and password using the `-u` and `-p` options, respectively:

```bash
dsbulk unload -k ks1 -t table1 \
  -u username -p password
```

If the cluster is remote, set the host and contact points in the [driver options](options/driver-options.md).

If your cluster has both [authentication and SSL enabled](options/driver-options.md#driverOptionsSsl), pass `-u`, `-p`, and the [SSL options](use-ssl.md).
For example:

```bash
dsbulk unload -h '["fe80::f861:3eff:fe1d:9d7a"]' \
  --driver.advanced.auth-provider.class DseGssApiAuthProvider \
  --driver.ssl.provider JDK \
  --driver.ssl.keystore.path /etc/dse/keystores/client.keystore \
  --driver.ssl.keystore.password mysslkspass \
  --driver.ssl.truststore.path /etc/dse/keystores/client.truststore \
  --driver.ssl.truststore.password myssltrustpass \
  -k mykeyspace -t mytable
```

### Authentication for self-managed clusters with a Mission Control CQL gateway

This option applies only to self-managed clusters that have a [Mission Control CQL gateway](https://docs.datastax.com/en/mission-control/databases/cql-gateway-connectivity.html) configured.

```bash
dsbulk unload -k ks1 -t table1 \
  -b "path/to/SCB.zip" -u username -p password
```

For Mission Control CQL gateway connections, the expected connection credentials are:

- `-b`: Provide the path to the [CQL gateway's Secure Connect Bundle (SCB)](https://docs.datastax.com/en/mission-control/databases/cql-gateway-connectivity.html#download-the-scb) zip file.
  The SCB includes certificates and key files for SSL-encrypted connections as well as information about the cluster's contact points.
- `-u`: The username to authenticate to the database.
- `-p`: The password to authenticate to the database.

### Authentication for Astra DB

```bash
dsbulk unload -k ks1 -t table1 \
  -b "path/to/SCB.zip" -u token -p AstraCS:...
```

For Astra DB, the expected connection credentials are:

- `-b`: Provide the path to the database's [Secure Connect Bundle (SCB)](https://docs.datastax.com/en/astra-db-serverless/databases/secure-connect-bundle.html) zip file.
  The SCB includes certificates and key files for SSL-encrypted connections as well as information about the database's contact points.
- `-u`: Set to the literal string `token`.
- `-p`: Provide an [application token](https://docs.datastax.com/en/astra-db-serverless/administration/manage-application-tokens.html).
  As a best practice, use secure references to tokens, such as environment variables, rather than specifying them directly on the command line.

## Set the unload destination

Use the `-url` option to specify the filesystem path or URL where you want to write the unloaded data.

### Unload to stdout

If `-url` is omitted or you set `-url -`, the unloaded data is written to standard output (`stdout`).

This is useful if you want to pipe the output from `stdout` to another command.
For example, you could pipe the output to an AWS CLI command to upload the data to an S3 bucket.

### Unload to a local directory

When `-url` is a filesystem path, the destination for `unload` is always treated as a directory.
If any directories in the path don't exist, DSBulk attempts to create them.

The `-url` unfurls to the long form of `file://hostname/path/to/directory`.

Relative paths are resolved from the current working directory.

If the path starts with `~/`, it expands to the user's home directory.

> [!TIP]
> The name of the output file is set by the `fileNameFormat` [connector option](options/connector-options.md).
> The default filename is `output-000001.csv`.
> If the `unload` operation creates multiple files, the counter in the filename increments for each file, such as `output-000002.csv`, `output-000003.csv`, and so on.
>
> If you run the same `unload` command multiple times to the same directory without changing the `fileNameFormat`, existing files are overwritten.

- Unload to a subdirectory under the current working directory:

  ```bash
  dsbulk unload -url myData -k ks1 -t table1
  ```

- Unload to a subdirectory under the user's home directory:

  ```bash
  dsbulk unload -url ~/data-export/myData -k ks1 -t table1
  ```

### Unload to a URL

If the destination begins with `http:` or `https:`, DSBulk attempts to write the unloaded data directly to the specified URL.
Some [connector options](options/connector-options.md) are ignored when the destination is a URL, such as `fileNameFormat`, and not all URLs are supported for unloading.

### Unload to a compressed file

There are two ways to unload compressed files:

- For supported file types, use the [`--connector.csv.compression`](options/connector-options.md#connectorCompression) or [`--connector.json.compression`](options/connector-options.md#jsonCompression) option to specify the compression type to allow DSBulk to create the compressed file.
- Pipe the `unload` command to your preferred compressor and filename:

  ```bash
  dsbulk unload -k ks1 -t table1 | gzip > table1.gz
  ```

## Unload data matching a CQL query

You can use the `-query` option to unload only data matching a given `SELECT` statement.

> [!IMPORTANT]
> Faceted queries are not supported.

- Unload data using a CQL query with a WHERE clause:

  ```bash
  dsbulk unload -query \
    "SELECT id, petal_length, petal_width, sepal_length, sepal_width, species
      FROM dsbulkblog.iris_with_id
      WHERE species='Iris-setosa' AND token(id) > :start AND token(id) <= :end"
  ```

- Unload data using a CQL query with function calls:

  ```bash
  dsbulk unload -query \
    "SELECT id, species, writetime(species) AS TIMESTAMP, ttl(species) AS TTL FROM dsbulkblog.iris_with_id"
  ```

- Unload data with a secondary index query:

  If a secondary index exists for a table, you can use a `SELECT` statement to unload data based on a given query on the secondary index.

  The following example uses `solr_query`.
  With DSE Search, continuous paging must be disabled because DSE Search doesn't work with that feature.
  The `solr_query` requires double-escaping of the double-quotes in the `solr_query` string.

  ```bash
  dsbulk unload -query \
    "SELECT id, petal_length, petal_width, sepal_length, sepal_width, species
      FROM dsbulkblog.iris_with_id
      WHERE solr_query = '{\\\"q\\\": \\\"species:Iris-setosa\\\"}'" \
    --executor.continuousPaging.enabled false
  ```

## Unload a materialized view (MV)

Specify the materialized view name in the `-t` (`--schema.table`) parameter.

The following command unloads and counts data from a materialized view to a local directory:

```bash
dsbulk unload count -k healthcare -t health_data_mv -url ~/export-mv
```

## Unload graph data

You can use DSBulk to unload data from graphs created with DSE Graph 6.8.0 and later.

To target graph data, you must specify one of the following:

- A graph name ([`--schema.graph`](options/schema-options.md#dsbulkSchemaGraph)) and a vertex label ([`--schema.vertex`](options/schema-options.md#dsbulkSchemaVertex)).
- A graph name (`--schema.graph`), edge label ([`--schema.edge`](options/schema-options.md#dsbulkSchemaEdge)), incoming vertex label ([`--schema.from`](options/schema-options.md#dsbulkSchemaFrom)), and outgoing vertex label ([`--schema.to`](options/schema-options.md#dsbulkSchemaTo)).
- A plain CQL query ([`--schema.query`](options/schema-options.md#dsbulkSchemaQuery)) that targets graph elements.
  Gremlin queries aren't supported.

The following example unloads graph data from a graph named `graph1` and a vertex label table named `vertex_label1` to a CSV file:

```bash
dsbulk unload --schema.graph graph1 --schema.vertex vertex_label1 \
  -delim '|' > unloaded_person_data.csv
```

## See also

- [CSV and JSON connector options](options/connector-options.md)
- [Schema options](options/schema-options.md)
- [Codec options](options/codec-options.md)
- [Driver options](options/driver-options.md)

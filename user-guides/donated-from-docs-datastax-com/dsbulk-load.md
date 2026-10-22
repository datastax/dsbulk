# Load data

Use the `dsbulk load` command to write data from a CSV or JSON file into a database table:

```bash
dsbulk load \
  -url SOURCE \
  -k KEYSPACE_NAME -t TABLE_NAME \
  OPTIONS
```

Replace the following:

- **`SOURCE`**: The path to the CSV or JSON data that you want to load.

  When you load a CSV file, the column names in the CSV data map to column names in the table unless another mapping is specified.

  When you load a JSON file, the field names in the JSON data map to column names in the table unless another mapping is specified.

- **`KEYSPACE_NAME`** and **`TABLE_NAME`**: The name of the keyspace (`-k`, `--schema.keyspace`) and table (`-t`, `--schema.table`) where you want to load the data.

  Or, use other [schema options](options/schema-options.md) to specify the target table, such as `-query` to select a table with a Cassandra Query Language (CQL) statement or `-g` to load graph data.

- **`OPTIONS`**: Any options for the `load` command, authentication options, or other [DSBulk options](02-usage.md).

> [!TIP]
> The [`--schema.mapping`](options/schema-options.md#schemaMapping) option is recommended for all `load` operations to ensure that CSV/JSON fields are mapped to table columns correctly.
>
> To test a `dsbulk load` operation without writing the data to your database, use the [`--dryRun`](options/engine-options.md#engine-dryRun) option.

## Load locally without authentication

The following command loads data from a local CSV file to a local cluster (`localhost` [contact point](options/driver-options.md)) without authentication:

```bash
dsbulk load -url filename.csv -k ks1 -t table1
```

## Load with authentication

Pass the relevant options with your `load` commands if your cluster requires authentication or uses SSL encryption.

You can pass the values directly or [use a configuration file](02-usage.md#use-a-configuration-file).

### Authentication for self-managed clusters (DSE, HCD, and Cassandra)

If your database requires authentication, provide the username and password using the `-u` and `-p` options, respectively:

```bash
dsbulk load -url filename.csv -k ks1 -t table1 \
  -u username -p password
```

If the cluster is remote, include [driver options](options/driver-options.md) like host, contact points, and port:

```bash
dsbulk load -url filename.csv -k ks1 -t table1 \
  -u username -p password -h '10.200.1.3, 10.200.1.4' -port 9876
```

If your cluster has both [authentication and SSL enabled](options/driver-options.md#driverOptionsSsl), pass `-u`, `-p`, and the [SSL options](use-ssl.md).
For example:

```bash
dsbulk load -h '["fe80::f861:3eff:fe1d:9d7a"]' -u myaccount -p mypassword \
  --driver.advanced.auth-provider.class DseGSSAPIAuthProvider \
  --driver.ssl.provider JDK \
  --driver.ssl.keystore.path /etc/dse/keystores/client.keystore \
  --driver.ssl.keystore.password mysslkspass \
  --driver.ssl.truststore.path /etc/dse/keystores/client.truststore \
  --driver.ssl.truststore.password myssltrustpass \
  -k mykeyspace -t mytable -url filename.csv
```

### Authentication for self-managed clusters with a Mission Control CQL gateway

This option applies only to self-managed clusters that have a [Mission Control CQL gateway](https://docs.datastax.com/en/mission-control/databases/cql-gateway-connectivity.html) configured.

```bash
dsbulk load -url filename.csv -k ks1 -t table1 \
  -b "path/to/SCB.zip" -u username -p password
```

For Mission Control CQL gateway connections, the expected connection credentials are:

- `-b`: Provide the path to the [CQL gateway's Secure Connect Bundle (SCB)](https://docs.datastax.com/en/mission-control/databases/cql-gateway-connectivity.html#download-the-scb) zip file.
  The SCB includes certificates and key files for SSL-encrypted connections as well as information about the cluster's contact points.
- `-u`: The username to authenticate to the database.
- `-p`: The password to authenticate to the database.

### Authentication for Astra DB

```bash
dsbulk load -url filename.csv -k ks1 -t table1 \
  -b "path/to/SCB.zip" -u token -p AstraCS:...
```

For Astra DB, the expected connection credentials are:

- `-b`: Provide the path to the database's [Secure Connect Bundle (SCB)](https://docs.datastax.com/en/astra-db-serverless/databases/secure-connect-bundle.html) zip file.
  The SCB includes certificates and key files for SSL-encrypted connections as well as information about the database's contact points.
- `-u`: Set to the literal string `token`.
- `-p`: Provide an [application token](https://docs.datastax.com/en/astra-db-serverless/administration/manage-application-tokens.html).
  As a best practice, use secure references to tokens, such as environment variables, rather than specifying them directly on the command line.

## Set the load source

Use the `-url` or `-urlfile` options to specify the source from which you want to load data.
This can be standard input, a local or remote file path, or a URL.

### Load a local file

To load a local file, provide the absolute or relative path to that file.

When loading a local file, the `-url` unfurls to the long form of `file://hostname/path/to/file`.
For example, loading from the `tmp` directory on `localhost` would be `file:///tmp/export.csv`.

Relative paths are resolved from the current working directory.

If the path starts with `~/`, it expands to the user's home directory.

- Load a local file from the current working directory:

  ```bash
  dsbulk load -url myData.csv -k ks1 -t table1
  ```

- Load a file from a different directory on the local file system:

  ```bash
  dsbulk load -url ~/data-export/myData.csv -k ks1 -t table1
  ```

### Load from a URL

Load data from remote targets by providing a valid URL to the CSV or JSON resource.

Use the format `-url https://hostname/path/to/filename.csv`.
For example:

```bash
dsbulk load -url https://192.168.1.100/data/export.csv
```

> [!IMPORTANT]
> When loading from a URL, the data is read directly and options like `recursive` and `filenamePattern` are ignored.
> For more information, see [CSV and JSON connector options](options/connector-options.md).

### <a id="load-from-aws-s3"></a>Load from AWS S3

You can use AWS S3 URLs to load data from AWS S3.

S3 URLs must contain the necessary query parameters for DSBulk to build an `S3Client` and access the target bucket:

- **`region` (Required)**: The AWS region, such as `us-west-1`.

  If your S3 URL provides the `region` only, then DSBulk uses the default AWS credentials provider to authenticate with role-based credentials.

- **`profile` (Recommended)**: A profile to use to provide [AWS SDK credentials](https://docs.aws.amazon.com/sdk-for-java/latest/developer-guide/credentials.html).

  You must provide either `profile` (recommended) or both `accessKeyId` and `secretKeyId`.

- **`accessKeyId` and `secretKeyId` (Not recommended)**: If you don't have a profile, you must use both of these parameters to provide your AWS S3 credentials.

  This option is less secure than `profile`, and it isn't recommended.
  Use `profile` whenever possible.

You can pass S3 URLs with the `-url` option or [`-urlfile`](#load-multiple-files) options.
However, certain options, like `recursive` and `fileNamePattern`, don't apply to URLs.
If your S3 bucket contains folders, you must either use the `urlfile` option to provide links for all folders, or run a separate `dsbulk load` command for each folder.

> [!TIP]
> DSBulk cannot unload directly to AWS S3.
> Instead, you can pipe the `dsbulk unload` output to a command that uploads the files to S3 using an AWS CLI, SDK, or API.

### Load multiple files

- Upload multiple files from a list:
  Create a local file containing a list of well-formed paths or URLs to load, with each target on a separate line.
  Then, set `-urlfile` to the path of that file:

  ```bash
  dsbulk load --connector.json.urlfile "multiple-input-data-urls.txt" -k ks1 -t table1
  ```

  Each entry is handled separately, and you can include URLs and file paths.
  For more information, see [CSV and JSON connector options](options/connector-options.md).

- Load all files from a directory:

  ```bash
  dsbulk load -url ~/export-dir -k ks1 -t table1
  ```

> [!TIP]
> DSBulk provides options to customize load behavior and file selection.
> For example, to load files from a directory and its subdirectory, use the `recursive` option, and to select files with a specific name pattern, use the `filenamePattern` option.
> For more information, see [CSV and JSON connector options](options/connector-options.md).

### <a id="load-data-from-stdin"></a>Load from stdin

If you omit `-url`, or you set `-url stdin:/`, then DSBulk reads data from standard input (`stdin`).

This is useful for loading data as it is generated or transformed by another process.

The following example runs a script called `generate_data`, and then pipes it to `dsbulk load`:

```bash
generate_data | dsbulk load -url stdin:/ -k ks1 -t table1
```

### Load a compressed file

There are two ways to load compressed files:

- For supported file types, use the [`--connector.csv.compression`](options/connector-options.md#connectorCompression) or [`--connector.json.compression`](options/connector-options.md#jsonCompression) option to specify the compression type to allow DSBulk to decompress the file as it is loaded.
- Extract the compressed file to `stdout`, and then pipe it from [`stdin`](#load-data-from-stdin) to your `dsbulk load` command:

  ```bash
  gzcat table1.csv.gz | dsbulk load -k ks1 -t table1
  ```

## Set headers or mappings

By default, DSBulk attempts to read the column or field names from a header row in the input file.

Use [`-header`](options/connector-options.md#connectorCSVHeader) to indicate whether the first row of a CSV file contains column names.

Use `-m` ([`--schema.mapping`](options/schema-options.md#schemaMapping)) to map field names or indices in the input file to column names in the table:

- Map CSV columns to table columns:

  ```bash
  dsbulk load -url export.csv -k ks1 -t table1 -header false -m '0=col1,1=col3'
  ```

- Map JSON fields to table columns.

  > [!TIP]
  > The following example also shows how a function call can be used to apply a function to one or more fields before mapping to a single column named `odd_column`.

  ```bash
  dsbulk load \
    -url /tmp/dsbulkblog/iris.json \
    -k dsbulkblog \
    -t iris_with_id \
    --schema.mapping "sepal_length = sepal_length, sepal_width = sepal_width, petal_length = petal_length, petal_width = petal_width, species = species, id = id, max(sepal_length, petal_length) = odd_column"
  ```

## Set a delimiter

Use `-delim` to specify a field delimiter character such as `'\t'` for <kbd>Tab</kbd>:

```bash
dsbulk load -k ks1 -t table1 -url export.csv -delim '\t'
```

## Set escaping

Use `-escape` to set handling for escaped characters.

The following example uses `-escape '\"'` to indicate that double-quoted characters in fields are escaped with a double-quote, such as the line `"f1","value with ""quotes"" and more"` in the CSV file:

```bash
dsbulk load -url ~/export.csv -k ks1 -t table1 -escape '\"'
```

## Set TTL

For more complex handling, you can pass CQL `INSERT` statements in the `-query` option.
A valuable way to use this option is to set time-to-live (TTL) values for the data you are loading.
For example:

```bash
dsbulk load -query \
  "INSERT INTO dsbulkblog.iris_with_id
    (sepal_length,sepal_width,petal_length,petal_width,species,id)
    VALUES (:sepal_length,:sepal_width,:petal_length,:petal_width,:species,:id)
    USING TTL :ttl_to_use"
```

## Load graph data

You can use DSBulk to load data into graphs created with DSE Graph 6.8.0 and later.

To target graph data, you must specify one of the following:

- A graph name ([`--schema.graph`](options/schema-options.md#dsbulkSchemaGraph)) and a vertex label ([`--schema.vertex`](options/schema-options.md#dsbulkSchemaVertex)).
- A graph name (`--schema.graph`), edge label ([`--schema.edge`](options/schema-options.md#dsbulkSchemaEdge)), incoming vertex label ([`--schema.from`](options/schema-options.md#dsbulkSchemaFrom)), and outgoing vertex label ([`--schema.to`](options/schema-options.md#dsbulkSchemaTo)).
- A plain CQL query ([`--schema.query`](options/schema-options.md#dsbulkSchemaQuery)) that targets graph elements.
  Gremlin queries aren't supported.

The following example loads graph data into a graph named `graph1` and a vertex label table named `vertex_label1`:

```bash
dsbulk load -url ~/data/vertices/person.csv -g graph1 -v vertex_label1 \
  -delim '|' -header true --schema.allowMissingFields true
```

## Load collections

DSBulk includes helpful simplification when loading collection data (`map`, `list`, `set` data types).
Collections inside a CSV file can contain valid JSON or simpler non-compliant JSON.

For example, given the following table:

```bash
CREATE TABLE t1 (col1 set<int> PRIMARY KEY, col2 list<int>, col3 map<text,int>);
```

The following pipe-delimited CSV file contains valid JSON, and you can load it into the table:

```plaintext
col1|col2|col3
[1,2,3]|[1,2,3]|{"key1":1,"key2":2}
```

The same CSV data can also be loaded in the following format, without the square brackets and curly braces:

```plaintext
col1|col2|col3
1,2,3|1,2,3|"key1":1,"key2":2
```

> [!IMPORTANT]
> Simplification doesn't work for nested collections.
>
> For example, if a column has the type `list<list<int>>`, then only the outermost structure can omit the surrounding characters.
> The following CSV file can be loaded as is:
>
> ```plaintext
> col1,col2,col3,col4
> [1,2,3]|[1,2,3]|{"key1":1,"key2":2}|[[1,2,3],[4,5,6]]
> ```
>
> Or it can be simplified as follows:
>
> ```plaintext
> col1,col2,col3,col4
> 1,2,3|1,2,3|"key1":1,"key2":2|[1,2,3],[4,5,6]
> ```
>
> No further simplification is possible because the fourth column is a nested collection.

## Load data into static and non-static columns

Use these steps to load database tables that contain static and non-static columns:

1. Create a dataset containing only the partition key columns and the static columns.
   For example, create a keyspace and a table:

   ```cql
   CREATE KEYSPACE demo WITH replication =
          {'class': 'SimpleStrategy', 'replication_factor': '1'};
   ```

   ```cql
   CREATE TABLE demo.mytable (
          pk1 text, pk2 text,
          cc1 text, cc2 text,
          static1 text static, static2 text static,
          nonstatic1 text, nonstatic2 text,
          PRIMARY KEY ((pk1,pk2), cc1, cc2));
   ```

2. Create a file `static-data.csv` containing the static data to load:

   ```plaintext
   pk1,pk2,static1,static2
   a,b,y,z
   ```

   > [!TIP]
   > By default, DSBulk considers the first row in the file as a CSV header.

3. Load static data with DSBulk using the `-query` parameter and the following format:

   ```bash
   -query "INSERT INTO t1 (pk1, pk2, ..., static1, static2, ...)"
   ```

   For example:

   ```bash
   dsbulk load -k demo -url static-data.csv \
     -query "INSERT INTO demo.mytable (pk1,pk2,static1,static2) \
     VALUES (:pk1,:pk2,:static1,:static2)"
   ```

4. Create another dataset `non-static-data.csv` containing the non-static data, including the entire primary key (partition keys and clustering columns) and regular columns:

   ```plaintext
   pk1,pk2,cc1,cc2,nonstatic1,nonstatic2
   a,b,c,d,e,f
   ```

5. Load non-static data using the `-query` parameter and the following format:

   ```bash
   -query "INSERT INTO t1 (pk1, pk2, .. cc1, cc2 ..., nonstatic1, nonstatic2, ...)"
   ```

   For example:

   ```bash
   dsbulk load -k demo -url non-static-data.csv \
     -query "INSERT INTO demo.mytable (pk1,pk2,cc1,cc2,nonstatic1,nonstatic2) VALUES (:pk1,:pk2,:cc1,:cc2,:nonstatic1,:nonstatic2)"
   ```

6. After the data loading operations complete, query the table with `cqlsh` or a driver to retrieve the static and nonstatic data:

   ```cql
   SELECT * from demo.mytable;
   ```

   ```console
    pk1 | pk2 | cc1 | cc2 | static1 | static2 | nonstatic1 | nonstatic2
   -----+-----+-----+-----+---------+---------+------------+------------
      a |   b |   c |   d |       y |       z |          e |          f
   ```

## See also

- [CSV and JSON connector options](options/connector-options.md)
- [Schema options](options/schema-options.md)
- [Batch options](options/batch-options.md)
- [Codec options](options/codec-options.md)
- [Driver options](options/driver-options.md)

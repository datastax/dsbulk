# Count data

Use the `dsbulk count` command to return information about loaded data:

```bash
dsbulk count \
  -k KEYSPACE_NAME -t TABLE_NAME \
  OPTIONS
```

Replace the following:

- **`KEYSPACE_NAME`** and **`TABLE_NAME`**: The name of the keyspace and table that contains the data you want to count.

  Or, use other [schema options](options/schema-options.md) to specify the data source, such as `-query` to select data with a Cassandra Query Language (CQL) statement or `-g` to count graph data.

- **`OPTIONS`**: Any options for the `count` command, such as [`--stats.modes`](#stats-modes), authentication options, or other [DSBulk options](02-usage.md).

## <a id="count-with-authentication"></a>Count with authentication

If you aren't running the `dsbulk count` command against a local cluster that doesn't require authentication, then you must provide authentication and connection details.

Pass the relevant options with your `count` commands if your cluster requires authentication or uses SSL encryption.

You can pass the values directly or [use a configuration file](02-usage.md#use-a-configuration-file).

### Authentication for self-managed clusters (DSE, HCD, and Cassandra)

If your database requires authentication, provide the username and password using the `-u` and `-p` options, respectively:

```bash
dsbulk count -k ks1 -t table1 \
  -u username -p password
```

If the cluster is remote, include [driver options](options/driver-options.md) like host, contact points, and port:

```bash
dsbulk count -k ks1 -t table1 \
  -u username -p password -h '10.200.1.3, 10.200.1.4' -port 9876
```

If your cluster has both [authentication and SSL enabled](options/driver-options.md#driverOptionsSsl), pass `-u`, `-p`, and the [SSL options](use-ssl.md).
For example:

```bash
dsbulk count -h '["fe80::f861:3eff:fe1d:9d7a"]' \
  --driver.advanced.auth-provider.class DseGssApiAuthProvider \
  --driver.ssl.provider JDK \
  --driver.ssl.keystore.path /etc/dse/keystores/client.keystore \
  --driver.ssl.keystore.password sslkspassword \
  --driver.ssl.truststore.path /etc/dse/keystores/client.truststore \
  --driver.ssl.truststore.password ssltrustpassword \
  -k ks1 -t table1
```

### Authentication for self-managed clusters with a Mission Control CQL gateway

This option applies only to self-managed clusters that have a [Mission Control CQL gateway](https://docs.datastax.com/en/mission-control/databases/cql-gateway-connectivity.html) configured.

```bash
dsbulk count -k ks1 -t table1 \
  -b "path/to/SCB.zip" -u username -p password
```

For Mission Control CQL gateway connections, the expected connection credentials are:

- `-b`: Provide the path to the [CQL gateway's Secure Connect Bundle (SCB)](https://docs.datastax.com/en/mission-control/databases/cql-gateway-connectivity.html#download-the-scb) zip file.
  The SCB includes certificates and key files for SSL-encrypted connections as well as information about the cluster's contact points.
- `-u`: The username to authenticate to the database.
- `-p`: The password to authenticate to the database.

### Authentication for Astra DB

```bash
dsbulk count -k ks1 -t table1 \
  -b "path/to/SCB.zip" -u token -p AstraCS:...
```

For Astra DB, the expected connection credentials are:

- `-b`: Provide the path to the [Astra DB database's Secure Connect Bundle (SCB)](https://docs.datastax.com/en/astra-db-serverless/databases/secure-connect-bundle.html) zip file.
  The SCB includes certificates and key files for SSL-encrypted connections as well as information about the database's contact points.
- `-u`: Set to the literal string `token`.
- `-p`: Provide an [application token](https://docs.datastax.com/en/astra-db-serverless/administration/manage-application-tokens.html).
  As a best practice, use secure references to tokens, such as environment variables, rather than specifying them directly on the command line.

## <a id="stats-modes"></a>Set the statistics to compute

Use the `--stats.modes` (`-stats`, `--dsbulk.stats.modes`) option to specify the statistics to compute:

- **`global` (default)**: Count the total number of rows in the table.

  `global` is the only `stats.modes` argument that is accepted with [`-query`](options/schema-options.md#dsbulkSchemaQuery), and the custom query is executed as given without parallelization.
  If you use `-k` and `-t` instead of `-query`, then you can use other `stats.modes` arguments.

- **`ranges`**: Count the total number of rows per token range in the table.
- **`hosts`**: Count the total number of rows per hosts in the table.
- **`partitions`**: Count the total number of rows in each partition, and then rank the partitions from largest (most rows) to smallest (least rows).

  By default, the top 10 partitions are returned.
  Use `--stats.numPartitions` (`-partitions`, `--dsbulk.stats.numPartitions`) to set the maximum number of partitions to return.

`stats.modes` is ignored if passed to `dsbulk load` or `dsbulk unload`.

## Count all rows

If you need to count all rows in a table, the `dsbulk count` command can be more efficient than a `SELECT COUNT(*)` CQL query, particularly for large tables.

The `dsbulk count` command is optimized for counting rows and can provide better performance and lower resource consumption compared to executing a CQL query that selects all rows.

A query such as `SELECT COUNT(*) FROM KEYSPACE_NAME.TABLE_NAME;` can be replaced by the following `dsbulk count` command:

```bash
dsbulk count -k KEYSPACE_NAME -t TABLE_NAME
```

The preceding example uses the default [`--stats.modes global`](#stats-modes) option to count all rows in the specified table.

## Count partition data

To count the number of rows in each partition and return information about the largest partitions, use [`--stats.modes partitions`](#stats-modes).

The following example gets information about the top 50 partitions in a table named `comments` that is in a keyspace named `cycling`:

```bash
dsbulk count -k cycling -t comments --stats.modes partitions --stats.numPartitions 50
```

Top partitions are determined by the number of rows in each partition.
If the table doesn't have 50 unique partitions, the command returns all partitions.

The output includes the log directory, performance metrics for the operation, and the data calculated by the operation.
The calculated data is presented in three columns:

- The first column is the partition key value.
- The second column is the number of rows using a given partition key value.
- The third column is the percentage of rows in the partition compared to the total number of rows that were scanned for the query.

```console
Operation directory: /home/automaton/cycling/logs/COUNT_20190424-213840-954894
total | failed | rows/s | mb/s | kb/row | p50ms | p99ms | p999ms
   31 |      0 |     74 | 0.00 |   0.02 | 27.59 | 31.33 |  31.33

Operation COUNT_20190424-213840-954894 completed successfully in 2 seconds.

fb372533-eb95-4bb4-8685-6ef61e994caa 5 16.13
8566eb59-07df-43b1-a21b-666a3c08c08a 4 12.90
c7fceba0-c141-4207-9494-a29f9809de6f 4 12.90
e7ae5cf3-d358-4d99-b900-85902fda9bb0 4 12.90
6ab09bec-e68e-48d9-a5f8-97e6fb4c9b47 3 9.68
9011d3be-d35c-4a8d-83f7-a3c543789ee7 2 6.45
95addc4c-459e-4ed7-b4b5-472f19a67995 2 6.45
38ab64b6-26cc-4de9-ab28-c257cf011659 2 6.45
5b6962dd-3f90-4c93-8f61-eabfa4a803e2 1 3.23
c4b65263-fe58-4846-83e8-f0e1c13d518f 1 3.23
e7cd5752-bc0d-4157-a80f-7523add8dbcd 1 3.23
6d5f1663-89c0-45fc-8cfd-60a373b01622 1 3.23
220844bf-4860-49d6-9a4b-6b5d3a79cbfb 1 3.23
```

## Count graph data

You can use the `dsbulk count` command to get information about graphs created with DSE Graph 6.8.0 or later.

To target graph data, you must specify one of the following:

- A graph name ([`--schema.graph`](options/schema-options.md#dsbulkSchemaGraph)) and a vertex label ([`--schema.vertex`](options/schema-options.md#dsbulkSchemaVertex)).
- A graph name (`--schema.graph`), edge label ([`--schema.edge`](options/schema-options.md#dsbulkSchemaEdge)), incoming vertex label ([`--schema.from`](options/schema-options.md#dsbulkSchemaFrom)), and outgoing vertex label ([`--schema.to`](options/schema-options.md#dsbulkSchemaTo)).
- A plain CQL query ([`--schema.query`](options/schema-options.md#dsbulkSchemaQuery)) that targets graph elements.
  Gremlin queries aren't supported.

To return all vertices or edges in a graph, use the default `global` option for [`--stats.modes`](#stats-modes).

- Count vertices:

  ```bash
  dsbulk count -g GRAPH_NAME -v VERTEX_LABEL
  ```

  The following example returns the number of `person` vertices that exist in the `person` vertex label table that is stored in the `food` graph:

  ```bash
  dsbulk count -g food -v person
  ```

- Count edges:

  ```bash
  dsbulk count -g GRAPH_NAME -e EDGE_LABEL
  ```

  The following example returns the number of `authored` edges that exist between `person` vertices and `book` vertices in the `authored` edge label table that are stored in the `food` graph:

  ```bash
  dsbulk count -g food -e authored -from person -to book
  ```

- Count graph data with a query:

  You can use [`-query`](options/schema-options.md#dsbulkSchemaQuery) to load, unload, or count graph data.
  Only plain CQL queries are supported.
  Gremlin queries aren't supported.

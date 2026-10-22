# Schema options

The `schema` options define the database table that is targeted by a DSBulk `load`, `unload`, or `count` operation, and how the schema is mapped between the table and a CSV or JSON file.
This includes the following:

- A keyspace name, table name, Cassandra Query Language (CQL) query, or graph elements.
- Field-to-column mappings.
- Handling for other schema-level parameters like timestamps and time-to-live (TTL).

> [!TIP]
> The [`--schema.mapping`](#schemaMapping) option is recommended for all `load` and `unload` operations to ensure that CSV/JSON fields are mapped to table columns correctly.
>
> To test a `dsbulk load` operation without writing the data to your database, use the [`--dryRun`](engine-options.md#engine-dryRun) option.

## Synopsis

The standard form for `schema` options is `--schema.KEY VALUE`:

- **`KEY`**: The specific option to configure, such as the `keyspace` option.
- **`VALUE`**: The value for the option, such as a string, number, or Boolean.

  HOCON syntax rules apply unless otherwise noted.
  For more information, see [Escape and quote DSBulk command line arguments](../02-usage.md#escape-and-quote-command-line-arguments).

### Short and long forms

On the command line, you can specify options in short form (if available), standard form, or long form.

For all `schema` options, the long form is the standard form with a `dsbulk.` prefix, such as `--dsbulk.schema.keyspace`.

The following examples show the same command with different forms of the `keyspace` and `table` options:

```bash
# Short form
dsbulk load -url filename.csv -k ks1 -t table1

# Standard form
dsbulk load -url filename.csv -schema.keyspace ks1 -schema.table table1

# Long form
dsbulk load -url filename.csv -dsbulk.schema.keyspace ks1 -dsbulk.schema.table table1
```

In [configuration files](../02-usage.md#use-a-configuration-file), you must use the long form with the `dsbulk.` prefix.
For example:

```json
dsbulk.schema.keyspace = "ks1"
dsbulk.schema.table = "table1"
```

## Core schema options

All `load`, `unload`, and `count` operations must specify a target for the operation as one of the following:

- A keyspace ([`--schema.keyspace`](#schemaKeyspace)) and table ([`--schema.table`](#dsbulkSchemaTable))
- A CQL statement ([`--schema.query`](#dsbulkSchemaQuery))
- [Graph schema options](#graph-schema-options) (DSE Graph 6.8.0 and later only)

When you use `--schema.keyspace` and `--schema.table`, DSBulk automatically generates the appropriate CQL statements based on the table's metadata and other `schema` options, and it uses all available columns.

When you use `--schema.query`, you can specify any valid CQL statement for loading, unloading, or counting data.
Validity is determined by CQL syntax rules and by the capabilities of DSBulk.

### <a id="schemaKeyspace"></a>--schema.keyspace (-k)

Specify the keyspace to use for a `load`, `unload`, or `count` operation.

In `--schema.keyspace`, keyspace names are case sensitive, and they must not be wrapped in quotes.
For example, `MyKeyspace` matches a keyspace named `MyKeyspace` but not `mykeyspace`.

Required if not using [`--schema.query`](#dsbulkSchemaQuery) or [graph schema options](#graph-schema-options).

Default: `null`

### <a id="dsbulkSchemaTable"></a>--schema.table (-t)

Specify the table to use for a `load`, `unload`, or `count` operation.

Required if not using [`--schema.query`](#dsbulkSchemaQuery) or [graph schema options](#graph-schema-options).

In `--schema.table`, table names are case sensitive, and they must not be wrapped in quotes.
For example, `MyTable` matches a table named `MyTable` but not `mytable`.

Default: `null`

### <a id="dsbulkSchemaQuery"></a>--schema.query (-query)

Provide a CQL statement to use for a `load`, `unload`, or `count` operation.

If your query doesn't include the keyspace name, you must specify the keyspace with `--schema.keyspace`.

If you don't use `-query`, then you must use either [`--schema.keyspace`](#schemaKeyspace) and [`--schema.table`](#dsbulkSchemaTable), or [graph schema options](#graph-schema-options) (for graph data only).

By default, `-query` is `null`.

#### `-query` with `dsbulk load`

For `load` operations, the `-query` statement can be any valid CQL `INSERT`, `UPDATE`, or `DELETE` statement.

`INSERT` statements are recommended for most `load` operations, with bound variables corresponding to mapped fields.
For example:

```bash
dsbulk load -query "INSERT INTO ks1.table1 (c1, c2, c3) VALUES (:fieldA, :fieldB, :fieldC)"
```

For counter tables, you must use `UPDATE` statements.
The columns are updated with incremental operations, such as `SET col1 = col1 + :fieldA`, where `:fieldA` is a column in the input data.

A `DELETE` statement removes existing data during the `load` operation.

#### `-query` with `dsbulk unload` and `dsbulk count`

For `unload` and `count` operations, the `-query` statement can be any regular `SELECT` statement.

> [!IMPORTANT]
> `SELECT *` (select all) statements are not recommended because they can have non-deterministic results as to which data is exported and in which order.
> This means the result can change when you rerun the same `SELECT *` statement.
>
> For databases that have DSE Search enabled, the pseudo-column `solr_query` can be exported by a `SELECT *` statement unintentionally.
> Without `-query`, DSBulk generates queries automatically, and it filters the `solr_query` pseudo-column transparently.
> If you use the `-query` option, you must explicitly filter the `solr_query` pseudo-column when querying tables that have DSE Search enabled.
>
> If you want to count or unload all rows, use `-k` and `-t` instead of `-query` so that DSBulk can generate the appropriate queries automatically.

If a `SELECT` statement _doesn't_ contain a `WHERE`, `ORDER BY`, `GROUP BY`, or `LIMIT` clause, then the DSBulk engine generates a token range restriction clause in the following format:

```console
WHERE token(COLUMNS) > :start and token(COLUMNS) <= :end
```

Then, the engine generates as many read range statements as there are token ranges in the cluster.
This allows parallelization of reads while simultaneously targeting coordinators that are also replicas (see [`--schema.splits`](##schemaOptionsschemasplits-number)).

If the `SELECT` statement _does_ contain a `WHERE`, `ORDER BY`, `GROUP BY`, or `LIMIT` clause, then the DSBulk engine executes the query as it is given without parallelization.
DSBulk can only parallelize the operation if the `WHERE` clause also includes the token range relation in the aforementioned format.
The token range can include one or more column names, and the bound variables can have any name.

`PER PARTITION LIMIT` clauses can be parallelized.

#### Bound variables in `-query`

`-query` statements can use both positional and named bound variables:

- **Named bound variables**: These are preferred unless the [protocol version](driver-options.md#datastaxJavaDriverAdvancedProtocolVersion) doesn't support them.

  Named bound variables usually have names that match the names of columns in the target table, but this isn't a strict requirement.
  However, these names must match the fields specified in [`--schema.mapping`](#schemaMapping).

- **Positional bound variables**: If positional variables are used, they are named after their corresponding columns in the target table.

  > [!IMPORTANT]
  > You cannot use positional variables in `SELECT` statements that have `WHERE` clause restrictions using `token()`, the `USING TIMESTAMP` clause, or the `USING TTL` clause.

The query is parsed to discover which bound variables are present, and then map the variables to their corresponding fields.
See [`--schema.mapping`](#schemaMapping) for more information about mapping bound variables.

#### Keyspace and table names in `-query`

Mixed-case keyspace and table names in `-query` statements must be double-quoted, and those quotes must be escaped.
For example:

```bash
dsbulk unload -query
  "SELECT id, row1, row2
    FROM ks1.\\\"tableMixedCase\\\"
    WHERE row1='some-value'"
```

### <a id="schemaMapping"></a>--schema.mapping (-m)

A string containing the field-to-column mapping to use for `load` and `unload` operations.

If not specified, DSBulk applies a strict one-to-one mapping between the CSV/JSON fields and the database table.
If that is not your intention, you must supply an explicit mapping.

The mapping must use the following format:

- **Indexed data sources**: Map zero-based field indexes to columns in the database table.

  For example, if the mapping is `0 = col1, 1 = col2, 2 = col3`, then `0`, `1`, and `2` are the zero-based indexes of fields in the CSV/JSON files,
  and `col1`, `col2`, and `col3` are the bound variable names that are used in the `INSERT`, `UPDATE`, or `SELECT` statement.

  If you are mapping the fields in order from `0` to `n`, you can specify the target columns only without the indexes, such as `col1, col2, col3`.

- **Mapped data sources**: Map field names to columns in the database table.

  For example, if the mapping is `fieldA = col1, fieldB = col2, fieldC = col3`, then `fieldA`, `fieldB`, and `fieldC` are the field names in the CSV/JSON files, and `col1`, `col2`, and `col3` are the bound variable name that are used in the `INSERT`, `UPDATE`, or `SELECT` statement.

  If the field names exactly match the column names, you can specify the target columns only without the field names, such as `col1, col2, col3`.

  When using mapped data sources, you can explicitly specify part of the mapping, and then generate the rest of the mapping automatically.
  For example, assume a CSV/JSON file has fields `c1`, `c2`, `c3`, and `c5`, and a table has columns `c1`, `c2`, `c3`, and `c4`.
  You can use the mapping `* = *, c5 = c4` to match the like-named columns automatically (`* = *`), and then manually map the unlike pair (`c5 = c4`).

  You can also selectively match like-named columns.
  For example, `* = -c2` matches all like-named fields except `c2`.
  Similarly, `* = [-c2, -c3]` matches all like-named fields except `c2` and `c3`.

In addition to the base syntax, you can use the following special syntax structures in the mapping:

- **Timestamp and TTL fields**: To use a field as the timestamp (`writetime`) or time-to-live (`ttl`) for an inserted row, you can use the reserved pseudo-columns `ttl(*)` and `writetime(*)` in the mapping.
  For example, `fieldA = writetime(*), fieldB = ttl(*)`.

  These aren't true columns; they are mapped as columns so DSBulk can parse the configuration correctly.

  > [!IMPORTANT]
  > Starting in DSBulk 1.8.0, the special tokens `__timestamp` and `__ttl` are deprecated but still honored.
  > If used, a warning message is logged.
  > Replace any `__timestamp` and `__ttl` tokens with `writetime(*)` and `ttl(*)` respectively.

  Timestamp fields can be parsed as CQL timestamp columns and must use the format specified in either [`--codec.timestamp`](codec-options.md#dsbulkCodecOptionsTimestamp) or both [`--codec.unit`](codec-options.md#dsbulkCodecOptionsUnit) and [`--codec.epoch`](codec-options.md#dsbulkCodecOptionsEpoch).

  TTL fields are parsed as integers representing the TTL duration in seconds, and they must use the format specified in [`--codec.number`](codec-options.md#dsbulkCodecOptionsNumber).

- **Values produced by function calls**: Your mapping can use the result of function calls as the value for loaded columns or unloaded fields.

  For `load` operations, to specify that a column is populated by the result of a function call, specify the function call as the input field.
  For example, `now() = c4` maps the result of the `now()` function to column `c4`.

  For `unload` operations, to specify that a field should be populated with the result of a function call, specify the function call as the input column.
  For example, `field1 = now()` maps the result of the `now()` function to field `field1`.

  Function calls can also be qualified by a keyspace name, such as `field1 = keyspace1.max(c1, c2)`.

- **Non-alphanumeric names**: In keeping with CQL grammar, any identifier, field, or column that isn't strictly alphanumeric (not matching `[a-zA-Z0-9_]+`) must be surrounded by double-quotes.
  To escape a double-quote, you can double it, such as `"Field ""A""" = "Column 2"`.
  However, unlike CQL grammar, DSBulk doesn't normalize unquoted identifiers to lowercase.
  This means that an identifier like `MyColumn1` matches a column named `MyColumn1` but not `mycolumn1`.

The type of mapping to use depends on the [connector](connector-options.md) being used.
Refer to the connector's documentation to determine whether it can produce only indexed records, only mapped records, or both.

## <a id="schemaAllowExtraFields"></a>--schema.allowExtraFields

For `load` operations only, this option specifies whether to accept records that contain extra fields that are not declared in the [`--schema.mapping`](#schemaMapping).
This option also applies to user-defined types (UDTs) and tuples missing elements declared in the mapping.

- **`true` (default)**: Records with missing fields or elements are accepted, and DSBulk ignores missing fields or elements.

  For example, if the mapping declares fields A, B, and C, but a record contains only fields A and B, then DSBulk considers field C to be null.

- **`false`**: The record is rejected if any field or element declared in the mapping is missing.

## <a id="schemaAllowMissingFields"></a>--schema.allowMissingFields

For `load` operations only, this option specifies whether to accept records that are missing fields declared in the [`--schema.mapping`](#schemaMapping).
This option also applies to user-defined types (UDTs) and tuples missing elements declared in the mapping.

> [!IMPORTANT]
> `dsbulk load` always rejects a record that is missing a field mapped to a primary key column because it can't be written to the database successfully.

- **`false` (default)**: The record is rejected if any field or element declared in the mapping is missing.
- **`true`**: Records with missing fields or elements are accepted, and DSBulk treats missing fields or elements as null.

  For example, if the mapping declares fields A, B, and C, but a record contains only fields A and B, then DSBulk considers field C to be null.

## <a id="schemaNullToUnset"></a>--schema.nullToUnset

Whether to map `null` input values to `unset` for `load` operations:

- **`true` (default)**: Map `null` input values to `unset` when performing `load` operations.

  Use this setting to avoid unintentionally modifying fields that might have pre-existing values.

  If the [protocol version](driver-options.md#datastaxJavaDriverAdvancedProtocolVersion) doesn't support unset values (all protocol versions less than 4), DSBulk overrides `--schema.nullToUnset` to `false` and logs a warning.

- **`false`**: Do not map `null` input values to `unset`.
  In this case, DSBulk creates tombstones to represent `null` values.

`--schema.nullToUnset` is applied after [`--codec.nullStrings`](codec-options.md#dsbulkCodecOptionsNullStrings), so `--schema.nullToUnset` can intercept nulls produced by `--codec.nullStrings`.

## <a id="schemaOptionspreserveTimestamp"></a>--schema.preserveTimestamp (-timestamp)

Whether to preserve timestamps when loading and unloading cells.

Ignored if [`--schema.query`](#dsbulkSchemaQuery) is used or the target table is a counter table.

- **`false` (default)**: Timestamps aren't preserved during `load` and `unload` operations.
- **`true`**: The following rules are applied to generated queries:
  - `load` operations generate a `BATCH` query instead of a single `INSERT` statement.
    A `BATCH` is required to preserve the individual column timestamps for each row.
  - `unload` operations generate a `SELECT` statement that exports each column along with its individual timestamp.

  For both loading and unloading, DSBulk imports and exports timestamps using field names formatted as `writetime(COLUMN)`, where **`COLUMN`** is the column's internal CQL name.
  For example, if the table has a column named `MyCol`, its corresponding timestamp is exported as `writetime(MyCol)` in the query generated by DSBulk and in the resulting [connector](connector-options.md) record.

  If you intend to use `--schema.preserveTimestamp` to export and import tables exclusively with DSBulk, then you don't need to change the automatically generated timestamp field names.

  However, if your workflow involves external sources that use different field names, use the `writetime` function in [`--schema.mapping`](#schemaMapping) to set the desired field name.
  For example, the mapping `field1 = col1, field1_writetime = writetime(col1)` maps `col1` and its timestamp to two distinct fields: `field1` and `field1_writetime`.

## <a id="schemaOptionsPreserveTtl"></a>--schema.preserveTtl (-ttl)

Whether to preserve the TTL when loading and unloading cells.

Ignored if [`--schema.query`](#dsbulkSchemaQuery) is used or the target table is a counter table.

- **`false` (default)**: TTLs aren't preserved during `load` and `unload` operations.
- **`true`**: The following rules are applied to generated queries:
  - `load` operations generate a `BATCH` query instead of a single `INSERT` statement.
    A `BATCH` is required to preserve the individual column TTLs for each row.
  - `unload` operations generate a `SELECT` statement that exports each column along with its individual TTL.

  For both loading and unloading, DSBulk imports and exports TTLs using field names formatted as `ttl(COLUMN)`, where **`COLUMN`** is the column's internal CQL name.
  For example, if the table has a column named `MyCol`, its corresponding TTL is exported as `ttl(MyCol)` in the query generated by DSBulk and in the resulting [connector](connector-options.md) record.

  If you intend to use `--schema.preserveTtl` to export and import tables exclusively with DSBulk, then you don't need to change the automatically generated TTL field names.

  However, if your workflow involves external sources that use different field names, use the `ttl` function in [`--schema.mapping`](#schemaMapping) to set the desired field name.
  For example, the mapping `field1 = col1, field1_ttl = ttl(col1)` maps `col1` and its TTL to two distinct fields: `field1` and `field1_ttl`.

## <a id="schemaQueryTimestamp"></a>--schema.queryTimestamp

A string specifying the timestamp for inserted and updated cells.

Only applies to `load` operations that _don't_ use [`--schema.query`](#dsbulkSchemaQuery).
If you use `--schema.query`, set the timestamp in that statement.

Allowed values include the following:

- **`null` or unspecified (default)**: The timestamp is based on the current system time in the environment where you are running DSBulk.
- **A date-time string**: Must use the format specified in [`--codec.timestamp`](codec-options.md#dsbulkCodecOptionsTimestamp).

> [!TIP]
> In Cassandra-based databases, https://cassandra.apache.org/doc/5.0/cassandra/managing/operating/fqllogging.html#query-information-logged[query timestamps (`queryState`)] have microsecond precision.
> If you specify a date-time string with higher precision, such as nanoseconds, the timestamp is set to the nearest microsecond and sub-microsecond information is lost.

## <a id="schemaQueryTtl"></a>--schema.queryTtl

A number specifying the [Time-To-Live (TTL)](https://cassandra.apache.org/doc/6.0-alpha2/cassandra/developing/cql/dml.html#insert-and-update-parameters) in seconds for inserted and updated cells.

Only applies to `load` operations that _don't_ use [`--schema.query`](#dsbulkSchemaQuery).
If you use `--schema.query`, set the TTL in that statement.

For no TTL, set to `-1`.

Default: `-1`

## <a id="schemaOptionsschemasplits-number"></a>--schema.splits

The number of token range splits in which to divide the token ring.
This setting determines how many read requests are generated when reading an entire table.
Only used when unloading and counting;
Ignored for `load` operations.

The actual number of splits may be slightly more or less than this value, depending on the actual cluster topology and token ownership.
Also, it is not possible to generate fewer splits than the total number of primary token ranges in the cluster.
Thus, the actual number of splits is always equal to or greater than that number.

Increase `--schema.splits` if you experience timeouts on `unload` and `count` commands, especially when [paging](driver-options.md#continuous-paging-options) is disabled.

This setting must be greater than [`--engine.maxConcurrentQueries`](engine-options.md#dsbulkEngineOptionsMaxConcurrentQueries).

Allowed values include the following:

- **Positive integer**: Specifies the exact number of splits to use, such as `4`.
- **`NC`**: A special syntax that you can use to set the number of splits as a multiple of the number of available cores for a given operation.
  For example, if you set `--schema.splits 0.5C` and there are 8 cores, then there will be 4 splits (`0.5 * 8 = 4`).

Default: `8C`

## <a id="graph-schema-options"></a>Graph schema options

You can use DSBulk to load, unload, and count graphs created with DSE Graph 6.8.0 and later.

To target graph data, you must specify one of the following:

- A graph name ([`--schema.graph`](#dsbulkSchemaGraph)) and a vertex label ([`--schema.vertex`](#dsbulkSchemaVertex)).
- A graph name (`--schema.graph`), edge label ([`--schema.edge`](#dsbulkSchemaEdge)), incoming vertex label ([`--schema.from`](#dsbulkSchemaFrom)), and outgoing vertex label ([`--schema.to`](#dsbulkSchemaTo)).
- A plain CQL query ([`--schema.query`](#dsbulkSchemaQuery)) that targets graph elements.
  Gremlin queries aren't supported.

### <a id="dsbulkSchemaGraph"></a>--schema.graph (-g)

Specify the graph name to use for `load`, `unload`, or `count` operations on DSE Graph data.

In `--schema.graph`, graph names are case sensitive, and they must not be wrapped in quotes.
For example, `MyGraph` matches a graph named `MyGraph` but not `mygraph`.

Required if not using [`--schema.query`](#dsbulkSchemaQuery) to target graph data.

Default: `null`

### <a id="dsbulkSchemaVertex"></a>--schema.vertex (-v)

Specify the vertex label to use for `load`, `unload`, or `count` operations on DSE Graph data.

The vertex label must correspond to an existing table created with the `VERTEX LABEL` option for `CREATE TABLE`.

In `--schema.vertex`, vertex labels are case sensitive, and they must not be wrapped in quotes.
For example, `MyVertex` matches a label named `MyVertex` but not `myvertex`.

Required if not using [`--schema.query`](#dsbulkSchemaQuery) or [`--schema.edge`](#dsbulkSchemaEdge) to target graph data.

Default: `null`

### <a id="dsbulkSchemaEdge"></a>--schema.edge (-e)

Specify the edge label to use for `load`, `unload`, or `count` operations on DSE Graph data.

The edge label must correspond to an existing table created with the `EDGE LABEL` option for `CREATE TABLE`.

In `--schema.edge`, edge labels are case sensitive, and they must not be wrapped in quotes.
For example, `MyEdge` matches a label named `MyEdge` but not `myedge`.

Required if not using [`--schema.query`](#dsbulkSchemaQuery) or [`--schema.vertex`](#dsbulkSchemaVertex) to target graph data.

If `--schema.edge` is specified, then [`--schema.from`](#dsbulkSchemaFrom) and [`--schema.to`](#dsbulkSchemaTo) are required.

Default: `null`

### <a id="dsbulkSchemaFrom"></a>--schema.from (-from)

For `load`, `unload`, or `count` operations on DSE Graph data, specify the name of the edge's incoming vertex label.

In `--schema.from`, vertex labels are case sensitive, and they must not be wrapped in quotes.
For example, `MyVertex` matches a label named `MyVertex` but not `myvertex`.

Required and relevant only if [`--schema.edge`](#dsbulkSchemaEdge) is specified.
Ignored when `--schema.edge` isn't used.

Default: `null`

### <a id="dsbulkSchemaTo"></a>--schema.to (-to)

For `load`, `unload`, or `count` operations on DSE Graph data, specify the name of the edge's outgoing vertex label.

In `--schema.to`, vertex labels are case sensitive, and they must not be wrapped in quotes.
For example, `MyVertex` matches a label named `MyVertex` but not `myvertex`.

Required and relevant only if [`--schema.edge`](#dsbulkSchemaEdge) is specified.
Ignored when `--schema.edge` isn't used.

Default: `null`

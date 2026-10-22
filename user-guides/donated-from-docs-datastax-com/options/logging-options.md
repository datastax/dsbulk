# Logging options

Use the `log` options to set logging behavior and error reporting settings for DSBulk commands.

## <a id="dsbulk-detection-of-write-failures"></a>Log storage and detection of write failures

General DSBulk log messages are written to the main DSBulk log file `operation.log` and standard error (`stderr`) only.
Logs aren't printed to `stdout`.

When running a specific `dsbulk` operation, the logs for that execution are written to a dedicated subdirectory within the [log directory](#log-directory).

For `load` and `unload` operations, DSBulk always prints a record's resource, which is the original file name (for `load` operations) or database table (for `unload` operations).
Additionally, if available, DSBulk always prints the record's position, which is the ordinal position of the record inside the resource, such as the record's line number in a CSV file.

Failed conditional writes, also known as Lightweight Transactions (LWT), Compare-And-Set (CAS), and the Paxos protocol, are written to a dedicated `paxos-errors.log` file.

For more detailed write failure diagnostics, you can enable the [`--log.sources`](#log-sources) option.

## Synopsis

The standard form for `log` options is `--log.TYPE.KEY VALUE` or `--log.KEY VALUE`:

- **`TYPE`**: If present, this segment indicates that an option applies to a specific type of log messages.
  Only used for the following groups of options:
  - [Checkpoint log options](#checkpoint-log-options): `--log.checkpoint.KEY VALUE`
  - [Row log options](#row-log-options): `--log.row.KEY VALUE`
  - [Statement log options](#statement-log-options): `--log.stmt.KEY VALUE`
- **`KEY`**: The specific option to configure, such as the `verbosity` option.
- **`VALUE`**: The value for the option, such as a string, number, or Boolean.

  HOCON syntax rules apply unless otherwise noted.
  For more information, see [Escape and quote DSBulk command line arguments](../02-usage.md#escape-and-quote-command-line-arguments).

### Short and long forms

On the command line, you can specify options in short form (if available), standard form, or long form.

For all `log` options, the long form is the standard form with a `dsbulk.` prefix, such as `--dsbulk.log.verbosity` or `--dsbulk.log.checkpoint.enabled`.

The following examples show the same command with different forms of the `verbosity` option:

```bash
# Short form
dsbulk load -verbosity 0 -url filename.csv -k ks1 -t table1

# Standard form
dsbulk load --log.verbosity 0 -url filename.csv -k ks1 -t table1

# Long form
dsbulk load --dsbulk.log.verbosity 0 -url filename.csv -k ks1 -t table1
```

In [configuration files](../02-usage.md#use-a-configuration-file), you must use the long form with the `dsbulk.` prefix.
For example:

```json
dsbulk.log.verbosity = 0
```

## General log options

The following options relate to general DSBulk logging behavior.
They use the [standard form](#synopsis) of `--log.KEY VALUE`.

### --log.ansiMode (-ansiMode)

Configure use of ANSI colors and other escape sequences in log messages printed to the console:

- **`normal` (default)**: Use ANSI colors if the terminal is detected as ANSI-compatible.
  Otherwise, ANSI isn't used.
  Compatible terminals include the following:
  - Terminals that are compatible with ANSI escape sequences, which includes all common terminals for *nix and BSD systems, as well as some popular terminals for Microsoft Windows (Mintty, MinGW).
  - The standard Windows DOS command prompt, which translates ANSI sequences on demand.

  For Windows, ANSI support works best with the Microsoft Visual C++ 2008 SP1 Redistributable Package installed.

- **`force`**: Always use ANSI colors even if the terminal isn't detected as ANSI-compatible.
- **`disable`**: Disable ANSI support in DSBulk log messages.

  This is necessary only if colored messages aren't desired or ANSI escape sequences cause printing errors.

### <a id="log-directory"></a>--log.directory (-logDir)

The relative or absolute path to the writable directory where DSBulk log files are stored.

Relative paths are resolved against the current working directory.
Paths that begin with a tilde (`~`) resolve to the current user's home directory, and then follow the path from there.

If the directory specified doesn't exist, DSBulk creates it.
The log files for each `dsbulk` run are stored in a subdirectory of the specified log directory.
Subdirectories are named after the [execution ID](engine-options.md#engine-executionId) of each run.

Must be a path.
URLs aren't accepted, including `file:` URLs.

Default: `./logs`

### --log.maxErrors (-maxErrors)

The maximum number of errors to tolerate before terminating the entire operation:

- **Positive integer**: Set the absolute number of errors to tolerate, such as `50`.
  Cannot be less than 0.
- **Percentage string**: Set the error threshold as a percentage of total rows processed at any given point in the operation.

  For example, setting this option to `"20%"` means that the operation stops if more than 20% of the rows processed result in errors.
  The absolute threshold to trigger this condition is recalculated as more rows are processed.
  For example, early in an operation, relatively few errors can trigger this condition, whereas, later in the operation, more errors are tolerated before hitting the percentage threshold.

  Use the format `"N%"`, where `N` is a decimal number between 0 and 100, exclusive.
  For example, allowed values include `"0.5%"`, `"33.3%"`, and `"50%"`, but not `"0%"` or `"100%"`.

- *`-1` (not recommended)*: Disable the error threshold and allow an infinite number of errors.

Default: `100`

### <a id="log-sources"></a>--log.sources

Whether to print the original source for processed records in debug files.

This information is collected in addition to the record's resource and position, which are always recorded by DSBulk, as explained in [Log storage and detection of write failures](#dsbulk-detection-of-write-failures).

> [!TIP]
> The `--log.sources` feature can make it easier to locate failed records and diagnose processing failures, particularly when the original data source is remote, such as an FTP or HTTP URL.

- **`true` (default)**: For each record that DSBulk failed to process, the debug files will contain the original source, such as the text line that the record was parsed from.

  For `load` operations, when `--log.sources true`, DSBulk creates `.bad` files to store the original lines for records that couldn't be written to the database.
  The name and content of the `.bad` files depends on where the failure occurred:
  - `connector.bad`: Contains records for failures that occur while the [connector](connector-options.md) is parsing data.
  - `mapping.bad`: Contains records for failures that occur while [mapping](schema-options.md#schemaMapping) data to a database table.
  - `load.bad`: Contains records for failures that occur while executing a CQL statement to insert data into a database table.
  - `paxos.bad`: Contains records for failures that occur when executing LWTs.

  `.bad` files are stored in execution-specific subdirectories within the [log directory](#log-directory).

  > [!TIP]
  > You can use DSBulk's `.bad` files as the data source for a subsequent `load` operation to retry failed records.

  The `--log.sources` feature requires that record sources are kept in memory until the record is fully processed.
  Retaining large records (more than 1 MB per record) in memory can put excessive pressure on the JVM heap, potentially causing out-of-memory (OOM) errors.
  This is exacerbated when batching is enabled.
  If you experience OOM errors, try disabling `--log.sources`, tuning or disabling [batching](batch-options.md), or tuning `connector` options like [`--connector.csv.maxColumns`](connector-options.md#connectorCSVmaxColumns), depending on the requirements for your DSBulk use case.

- **`false`**: Detailed record sources aren't printed in debug files, and `load` operations don't create `.bad` files for failed records.

### <a id="log-verbosity"></a>--log.verbosity (-verbosity)

Set the overall level of verbosity for logs:

- `0` (`quiet`): Only log `WARN` and `ERROR` messages.
- `1` (`normal`): Log `WARN`, `ERROR`, and some `INFO` messages.
- `2` (`high`): Log all `INFO`, `WARN`, and `ERROR` messages, as well as any `DEBUG` messages from the beginning of the operation, such as DSBulk settings, inferred query, and consistency level.

  Only use `high` while diagnosing problems related to configuration or performance in production environments.
  Revert to a lower verbosity level once the diagnosis is complete.

  > [!TIP]
  > Log level `2` prints basic cluster information that can help with troubleshooting.
  > For example:
  >
  > ```console
  > Partitioner: name-of-partitioner
  > Total number of hosts: number
  > DataCenters: list-of-datacenter-names
  > Hosts: list-of-hosts: address, dseVersion, cassandraVersion, dataCenter
  > ```
  >
  > The output is sorted by ascending IP address.
  > If there are more than 100 nodes comprising the cluster, nodes beyond the 100th node aren't listed, and DSBulk prints `(Other nodes omitted)`.

- `max`: Maximum verbosity including `INFO`, `WARN`, `ERROR`, and many `DEBUG` messages from DSBulk, the driver, and important libraries like Netty and Reactor.

  > [!WARNING]
  > Don't use `max` in production environments.
  > Only use `max` for debugging in lower environments on small amounts of data.
  > When used on full-scale production data, it can produce hundreds of gigabytes of log data in the main log file and console.

Log messages are only logged to the main log file, `operation.log`, and to standard error.
Nothing from the log is printed to `stdout`.

For CQL statement log verbosity, see [`--log.stmt.level`](#log-stmt-level).

## <a id="checkpoint-log-options"></a>Checkpoint log options

The following options relate to logs for operation checkpoints.

### <a id="dsbulkLoggingOptionsCheckpointEnabled"></a>--log.checkpoint.enabled

Whether to allow DSBulk to checkpoint the current operation.

- **`true` (default)**: DSBulk can track records that are processed, and then produce a checkpoint file at the end of the operation.

  You can use the checkpoint file with [`--log.checkpoint.file`](#dsbulkLoggingOptionsCheckpointFile) to resume the same operation if some records weren't processed or the operation was interrupted.

  > [!IMPORTANT]
  > With checkpoints enabled, DSBulk operations consume more memory and can take longer to complete.
  > Disable checkpoints when they aren't needed.

- **`false`**: DSBulk doesn't produce checkpoint files, and it can't resume operations from a checkpoint.

### <a id="dsbulkLoggingOptionsCheckpointFile"></a>--log.checkpoint.file

Resume an operation by providing the path to a previously created checkpoint file (see [`--log.checkpoint.enabled`](#dsbulkLoggingOptionsCheckpointEnabled)).

If `--log.checkpoint.file` is specified, the operation handles only the unprocessed or failed data from the previous checkpoint, depending on [`--log.checkpoint.replayStrategy`](#dsbulkLoggingOptionsCheckpointReplayStrategy).

Additionally, if `--log.checkpoint.file` is specified, the command must target the same dataset as the original operation:

- For `load` operations, the command must target the same data source (`-url` or `-urlfile`).

  Make sure the source wasn't renamed, moved, or changed.
  If the source was moved or renamed, then all files are considered new and loaded in their entirety; the checkpoint file is ignored.
  If the contents changed, then new records can go unnoticed or cause other records to be processed twice.

- For `unload` operations, the command must target the same database table.

  Make sure that the read query, the token distribution across the ring, the number of [`--schema.splits`](schema-options.md#schemaOptionsschemasplits-number), and the data to read are identical across operations.
  If there are inconsistencies, the results of the resumed unload operation can be inconsistent.

Default: `null` (no checkpoint file used)

### <a id="dsbulkLoggingOptionsCheckpointReplayStrategy"></a>--log.checkpoint.replayStrategy

The replay strategy to use when resuming an operation from a checkpoint file with [`--log.checkpoint.file`](#dsbulkLoggingOptionsCheckpointFile):

- **`resume` (safest for non-idempotent operations)**: DSBulk only processes new records from resources that weren't consumed entirely.
  Records that were already processed are ignored, including rejected ones.

  This is the safest option for non-idempotent `load` operations.

  Rejected records are written to `.bad` files for `load` operations where [`--log.sources true`](#log-sources).

- **`retry` (default)**: DSBulk processes new and rejected records from resources that weren't consumed entirely.

  > [!IMPORTANT]
  > This strategy can cause some rows to be inserted twice.
  > Only use this option for idempotent operations.

- **`retryAll`**: DSBulk processes new and rejected records from _all_ resources, including those that were marked as consumed entirely in the checkpoint file.

  > [!IMPORTANT]
  > This strategy can cause some rows to be inserted twice.
  > Only use this option for idempotent operations.

## <a id="row-log-options"></a>Row log options

The following options relate to logs for rows processed during a DSBulk operation.

### --log.row.maxResultSetValues

Set the maximum number of result set values to print from rows returned by `dsbulk` queries on the database:

- **Positive integer**: A fixed maximum number of result set values.
  If a row has more result set values than this limit, the excess values aren't printed by DSBulk.
- **`-1` (not recommended)**: Allow an unlimited number of result set values.

Default: `50`

### --log.row.maxResultSetValueLength

Set the maximum length for result set values to print from rows returned by `dsbulk` queries on the database:

- **Positive integer**: A fixed maximum length for a result set value.
  Rows with result set values longer than this limit are truncated.
- **`-1` (not recommended)**: Allow unlimited length for result set values.

Default: `50`

## <a id="statement-log-options"></a>Statement log options

The following options relate to logs for CQL statements executed during a DSBulk operation.

### --log.maxQueryWarnings

> [!IMPORTANT]
> This option is related to statement log options, but it doesn't include the `stmt` segment in its name.
> The [long form](#synopsis) for this option is `--dsbulk.log.maxQueryWarnings`.

The maximum number of query warnings to log before muting them.

Query warnings are sent by the server when, for example, the number of statements in a batch is greater than the warning threshold configured on the server.
These can help diagnose suboptimal configurations, but they can be noisy and unnecessary.

- **Positive integer**: A fixed maximum number of query warnings to log.

  DSBulk logs query warnings up to this limit, and then mutes query warnings and stops logging them.

- **Negative integer (not recommended)**: Allow unlimited query warnings.

Default: `50` (log the first 50 query warnings only)

### <a id="log-stmt-level"></a>--log.stmt.level

The verbosity level for DSBulk CQL statement logs:

- **`ABRIDGED`**: Print only basic information in summarized form.
- **`NORMAL`**: Print basic information in summarized form and the statement's query string, if available.
  For batch statements, this level also prints information about the batch's inner statements.
- **`EXTENDED` (default)**: Print full information, including the statement's query string and bound values, if available.
  For batch statements, this level also prints all information available about the batch's inner statements.

For general log verbosity, see [`--log.verbosity`](#log-verbosity).

### --log.stmt.maxBoundValueLength

The maximum length for bound values:

- **Positive integer**: A fixed maximum length for bound values.
  Bound values longer than this limit are truncated.
- **`-1` (not recommended)**: Allow unlimited length for bound values.

Default: `50`

### --log.stmt.maxBoundValues

The maximum number of bound values to print:

- **Positive integer**: A fixed maximum number of bound values per statement.
  If the statement has more bound values than this limit, the excess values aren't printed.
- **`-1` (not recommended)**: Allow an unlimited number of bound values per statement.

Default: `50`

### --log.stmt.maxInnerStatements

The maximum number of inner statements to print for a batch statement.
Only applicable for batch statements.

- **Positive integer**: A fixed maximum number of inner statements per batch statement.
  If the batch statement has more inner statements than this limit, the excess inner statements aren't printed.
- **`-1` (not recommended)**: Allow an unlimited number of inner statements per batch statement.

Default: `10`

### --log.stmt.maxQueryStringLength

The maximum length for a query string:

- **Positive integer**: A fixed maximum length for query strings.
  Query strings longer than this limit are truncated.
- **`-1` (not recommended)**: Allow unlimited length for query strings.

Default: `500`

## See also

- [Engine options](engine-options.md)
- [Monitoring options](monitor-options.md)

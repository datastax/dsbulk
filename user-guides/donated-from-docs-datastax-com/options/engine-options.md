# Engine options

Use the `engine` options for general performance tuning and throughput for `dsbulk` operations.

For query-level tuning, results paging, and other driver performance settings, see [Driver options](driver-options.md).

## Synopsis

The standard form for `engine` options is `--engine.KEY VALUE`:

- **`KEY`**: The specific option to configure, such as the `dryRun` option.
- **`VALUE`**: The value for the option, such as a string, number, or Boolean.

  HOCON syntax rules apply unless otherwise noted.
  For more information, see [Escape and quote DSBulk command line arguments](../02-usage.md#escape-and-quote-command-line-arguments).

### Short and long forms

On the command line, you can specify options in short form (if available), standard form, or long form.

For all `engine` options, the long form is the standard form with a `dsbulk.` prefix, such as `--dsbulk.engine.dryRun`.

The following examples show the same command with different forms of the `dryRun` option:

```bash
# Short form
dsbulk load -dryRun true -url filename.csv -k ks1 -t table1

# Standard form
dsbulk load --engine.dryRun true -url filename.csv -k ks1 -t table1

# Long form
dsbulk load --dsbulk.engine.dryRun true -url filename.csv -k ks1 -t table1
```

In [configuration files](../02-usage.md#use-a-configuration-file), you must use the long form with the `dsbulk.` prefix.
For example:

```json
dsbulk.engine.dryRun = true
```

## --engine.dataSizeSamplingEnabled

Whether to use data size sampling to optimize the DSBulk execution engine.

- **`true` (default)**: Enable data size sampling for applicable `dsbulk load` commands.

  This option only applies to `dsbulk load` commands that read from a file, directory, or HTTP/HTTPS URL.
  Data size sampling never occurs for `dsbulk unload`, `dsbulk count`, or `dsbulk load` commands that read from standard input (`-url -`).

- **`false`**: Disable data size sampling.

  > [!IMPORTANT]
  > Set `--engine.dataSizeSamplingEnabled false` if your data source cannot be read multiple times.
  >
  > To perform data size sampling, DSBulk invokes the [connector](connector-options.md) to read a few sample records and estimate the total data size.
  > Then, DSBulk invokes the connector again to read the entire body of data and perform the `load` operation.
  >
  > Because DSBulk must invoke the connector twice, data size sampling is only possible if the data source can be read multiple times.
  > DSBulk can't complete the `load` operation if it cannot reread the entire data source, such as short-lived ephemeral files and single-use URLs.

## <a id="engine-dryRun"></a>--engine.dryRun (-dryRun)

Whether to run a `dsbulk load` command as a dry run (simulation) without actually modifying the database.

- **`false` (default)**: Disable dry-run mode.
  The `load` operation runs normally, and the data is written to the database.
- **`true`**: Enables dry-run mode so you can test a `load` operation without writing data to the database.

## <a id="engine-executionId"></a>--engine.executionId

Set a unique identifier for each `dsbulk load`, `dsbulk unload`, and `dsbulk count` operation.

- **`null` (default), unspecified, or empty**: The DSBulk engine automatically generates identifiers in the format `WORKFLOW_TIMESTAMP`, such as `LOAD_20240315-142530-123456`:
  - **`WORKFLOW`**: The operation type, either `LOAD`, `UNLOAD`, or `COUNT`.
  - **`TIMESTAMP`**: The UTC time that the workflow started, formatted as [`uuuuMMdd-HHmmss-SSSSSS`](https://docs.oracle.com/javase/8/docs/api/java/time/format/DateTimeFormatter.html#patterns), with microsecond precision, if available, or millisecond precision otherwise.
- **User-defined string**: A user-supplied identifier, such as an environment variable or template.

  > [!IMPORTANT]
  > If the execution ID is user-defined, you must guarantee its uniqueness.
  > Duplicate execution IDs can cause commands to fail.

  To specify a template, the format must comply with formatting rules for [`String.format()`](https://docs.oracle.com/javase/8/docs/api/java/util/Formatter.html#syntax), and it can use the following variables:
  - **`%1$s`**: A variable representing the workflow type.
    At runtime, this resolves to `LOAD`, `UNLOAD`, or `COUNT`.
  - **`%2$t`**: A variable representing the UTC time that the workflow started, formatted as [`uuuuMMdd-HHmmss-SSSSSS`](https://docs.oracle.com/javase/8/docs/api/java/time/format/DateTimeFormatter.html#patterns), with microsecond precision, if available, or millisecond precision otherwise.
  - **`%3$s`**: A variable representing the JVM process PID for the operation.
    This parameter isn't available on all operating systems.
    If the PID cannot be determined, DSBulk inserts a random integer instead.

  You can use a template to change the default identifier format.
  For example, to add a `test-` prefix, set `--engine.executionId "test-%1$s-%3$s"`. This produces an identifier such as `test-LOAD-62` (workflow type and process ID).

## <a id="dsbulkEngineOptionsMaxConcurrentQueries"></a>--engine.maxConcurrentQueries (-maxConcurrentQueries)

The maximum number of concurrent queries to run in parallel.

This option is a safeguard that can prevent a command from starting more concurrent queries than the cluster can handle.
You can also use this option to regulate throughput if latency is too high.

Allowed values for `--engine.maxConcurrentQueries` include the following:

- **`AUTO` (default)**: DSBulk calculates an optimal number of concurrent queries according to the number of available cores and the operation being executed.
  Typically, the resulting value is no more than 8 times the number of available cores.
- **`NC`**: A special syntax that you can use to set the value as a multiple of the number of available cores for a given operation.
  For example, if you set `-maxConcurrentQueries 10C` and there are 8 cores, then there can be 80 parallel queries (`10 * 8 = 80`).
- **Positive integer**: Specify an exact number of concurrent queries allowed to run in parallel, such as `100`.

> [!IMPORTANT]
> - When using [continuous paging](driver-options.md#continuous-paging-options), make sure `-maxConcurrentQueries` doesn't exceed the number of nodes in the local datacenter multiplied by the server-side value of `continuous_paging.max_concurrent_sessions` in your cluster's `cassandra.yaml` file:
>
>   ```console
>   maxConcurrentQueries <= (local datacenter nodes) * (continuous_paging.max_concurrent_sessions)
>   ```
>
>   If `-maxConcurrentQueries` is too high, requests can be rejected due to overloaded server resources.
>
> - Astra DB rate limits are always enforced on the Astra DB server side, regardless of the DSBulk configuration.
>   If your `dsbulk` commands connect to an Astra DB database, make sure this limit doesn't exceed the Astra DB rate limit.
> - `--engine.maxConcurrentQueries` is recommended to control throughput instead of the following deprecated options:
>   - [`--dsbulk.executor.maxInFlight`](driver-options.md#dsbulkExecutorOptionsMaxInFlight)
>   - [`--dsbulk.executor.maxPerSecond`](driver-options.md#dsbulkExecutorOptionsMaxPerSecond)
>   - [`--dsbulk.executor.continuousPaging.maxConcurrentQueries`](driver-options.md#executor-continuousPaging-maxConcurrentQueries)

To check the `--engine.maxConcurrentQueries` setting, set [`-verbosity 2`](logging-options.md#log-verbosity), run a `load`, `unload`, or `count` command, and then check the `operation.log` file for a line starting with `Using read concurrency:` or `Using write concurrency:`.

For the purposes of this option, [batch statements](batch-options.md) are considered one query.

## --s3.clientCacheSize

> [!TIP]
> The `--s3.clientCacheSize` (`--dsbulk.s3.clientCacheSize`) option doesn't belong to the `engine` options group.
> It's documented here because it relates to the core configuration for `dsbulk`.
> However, it's unlikely that this option would have an impact on performance.

When using the `urlfile` option with AWS S3 URLs, DSBulk creates an S3 client for each bucket specified in the S3 URLs.
DSBulk caches the S3 clients to prevent them from being re-created unnecessarily when processing many S3 URLs that target the same buckets.
If all of your S3 URLs target the same bucket, then the same S3 client is used for each URL, and the cache contains only one entry.
The size of the S3 client cache is controlled by the `--s3.clientCacheSize` (`--dsbulk.s3.clientCacheSize`) option, and the default is `20` entries.
The default value is arbitrary, and it only needs to be changed when loading from many different S3 buckets in a single command.

For more information, see [Load from AWS S3](../dsbulk-load.md#load-from-aws-s3) and [`--connector.csv.urlfile`](connector-options.md#connectorUrlFile) or [`--connector.json.urlfile`](connector-options.md#jsonUrlFile).

## --runner.promptForPasswords

You can use the `--runner.promptForPasswords` (`--dsbulk.runner.promptForPasswords`) option to control whether DSBulk prompts for passwords that are missing from configuration files:

- **`true` (default)**: If a username is present in the configuration, but the corresponding password is missing, DSBulk prompts the user to enter the password on the command line.

  Prompting for passwords requires an interactive shell.
  If the standard input isn't connected to a terminal, DSBulk doesn't prompt for passwords, even if this option is enabled.

  Because DSBulk bypasses password prompting when it's impossible, only disable this option if DSBulk assumes incorrectly that it is running in an interactive shell.

- **`false`**: DSBulk never prompts for missing passwords.

## See also

- [Driver options](driver-options.md)
- [Logging options](logging-options.md)
- [Monitoring options](monitor-options.md)
- [`--connector.csv.maxColumns`](connector-options.md#connectorCSVmaxColumns)
- [`--connector.csv.maxCharsPerColumn`](connector-options.md#connectorCSVmaxCharsPerColumn)
- [Prompt for passwords](../02-usage.md#prompt-for-passwords)

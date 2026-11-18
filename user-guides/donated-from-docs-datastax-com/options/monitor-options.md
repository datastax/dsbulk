# Monitoring options

The `monitoring` options determine which metrics are collected and reported for `load`, `unload`, and `count` operations.

The following example is a typical metrics report for a `load` workflow:

```console
2018-03-14 13:15:48 INFO Memory usage: used: 507 MB, free: 691 MB, allocated: 1,199 MB, available: 3,641 MB,
    total gc count: 20, total gc time: 346 ms
2020-06-14 13:15:48 INFO Records: total: 210,755, successful: 210,755, failed: 0, mean: 20,893 records/second
2020-06-14 13:15:48 INFO Batches: total: 6,602, size: 31.90 mean, 10 min, 32 max
2020-06-14 13:15:48 INFO Writes: total: 210,669, successful: 210,669, failed: 0, in-flight: 0
2020-06-14 13:15:48 INFO Throughput: 20,877 writes/second, 1.11 mb/second
2020-06-14 13:15:48 INFO Latencies: mean 6.29, 75p 2.87, 99p 89.13, 999p 125.83 milliseconds
```

> [!TIP]
> Monitored throughput is often measured as operations per second, where an operation is a single write event or a single read event.
> This measurement can vary greatly depending on the size of the row being written or read.
>
> To avoid this irregularity, consider using `mb/sec` (megabytes per second) as a different measure of throughput.

## Synopsis

The standard form for `monitoring` options is `--monitoring.KEY VALUE`:

- **`KEY`**: The specific option to configure, such as the `console` option.
- **`VALUE`**: The value for the option, such as a string, number, or Boolean.

  HOCON syntax rules apply unless otherwise noted.
  For more information, see [Escape and quote DSBulk command line arguments](../02-usage.md#escape-and-quote-command-line-arguments).

### Short and long forms

On the command line, you can specify options in short form (if available), standard form, or long form.

For all `monitoring` options, the long form is the standard form with a `dsbulk.` prefix, such as `--dsbulk.monitoring.console`.

The following examples show the same command with different forms of the `jmx` option:

```bash
# Short form
dsbulk count -jmx false -k ks1 -t table1

# Standard form
dsbulk count --monitoring.jmx false -k ks1 -t table1

# Long form
dsbulk count --dsbulk.monitoring.jmx false -k ks1 -t table1
```

In [configuration files](../02-usage.md#use-a-configuration-file), you must use the long form with the `dsbulk.` prefix.
For example:

```json
dsbulk.monitoring.jmx = false
```

## <a id="monitoring-console"></a>--monitoring.console

Whether to stream metrics to the console.

- **`true` (default)**: Enable the console reporter.

  DSBulk prints useful metrics about the ongoing operation to standard error, including total records, failed records, throughput, latency, and average batch size (if [batching](batch-options.md) is used).
  The metrics are refreshed at the interval set in [`-reportRate`](#dsbulkMonitoringOptionsReportRate).

  If [`--log.verbosity quiet`](logging-options.md#log-verbosity) (or the deprecated `0`), DSBulk disables the console reporter regardless of the value of `--monitoring.console`.

- **`false`**: Disable the console reporter.

## --monitoring.csv

Whether to print metrics to CSV files in the [DSBulk log directory](logging-options.md#log-directory):

- **`false` (default)**: Don't output metrics to CSV files.
- **`true`**: Output metrics to CSV files.

## --monitoring.durationUnit

Specify the time unit to use for latency duration metrics.

Accepts any [`TimeUnit`](https://docs.oracle.com/javase/8/docs/api/java/util/concurrent/TimeUnit.html#enum.constant.summary) enum constant.

Default: `MILLISECONDS`

## --monitoring.expectedReads

Set the expected total number of reads.
If set, the console reporter also prints the overall achievement percentage for reads.

Set to `-1` to disable this feature.

Default: `-1` (disabled)

## --monitoring.expectedWrites

Set the expected total number of writes.
If set, the console reporter also prints the overall achievement percentage for writes.

Set to `-1` to disable this feature.

Default: `-1` (disabled)

## <a id="monitoring-jmx"></a>--monitoring.jmx (-jmx)

Whether to use JMX reporting:

- **`true` (default)**: Enable JMX reporting.

  Remote JMX reporting requires several properties to be set in the JVM at launch.
  This is accomplished through the `DSBULK_JAVA_OPTS` environment variable.

  JMX reporting can also expose driver metrics.
  However, driver metrics are disabled by default.
  To enable driver metrics, you must set the following [driver options](driver-options.md):
  - [`datastax-java-driver.advanced.metrics.node.enabled`](driver-options.md#datastaxJavaDriverAdvancedMetricsNodeEnabled): Provide a list of node-level metrics to enable, specified by metric name.
  - [`datastax-java-driver.advanced.metrics.session.enabled`](driver-options.md#datastaxJavaDriverAdvancedMetricsSessionEnabled): Provide a list of session-level metrics to enable, specified by metric name.

  Driver metrics are stored in a directory named after the driver `session` name.
  The default session name for DSBulk is `driver`.

- **`false`**: Disable JMX reporting.

## --monitoring.rateUnit

Specify the time unit to use for throughput rate metrics.
For example, if `--monitoring.rateUnit "SECONDS"`, then throughput rates are measured in rows per second.

Accepts any [`TimeUnit`](https://docs.oracle.com/javase/8/docs/api/java/util/concurrent/TimeUnit.html#enum.constant.summary) enum constant.

Default: `SECONDS`

## <a id="dsbulkMonitoringOptionsReportRate"></a>--monitoring.reportRate (-reportRate)

The report interval for the [console reporter](#monitoring-console).
For example, if set to `10 seconds`, DSBulk prints updated metrics to the console every 10 seconds.

Accepts any value in [HOCON duration format](https://github.com/lightbend/config/blob/master/HOCON.md#duration-format).

Values less than 1 second are automatically rounded up to 1 second.

Default: `5 seconds`

## <a id="dsbulkMonitoringOptionsTrackBytes"></a>--monitoring.trackBytes

Whether to track throughput in bytes per second.

> [!IMPORTANT]
> Although bytes per second can be useful for evaluating data transfer, computing this metric is CPU-intensive.
> Enabling this option can impact performance, including increased latency and reduced throughput.
>
> If you enable this option, watch for new performance degradations that don't align with normal workloads.
> Additionally, be aware that the heuristic used to compute data sizes isn't perfectly accurate, and it can sometimes underestimate the actual size.

- **`false` (default)**: DSBulk measures throughput as the number of records written or read per second.
- **`true`**: DSBulk measures throughput as the number of bytes sent or received per second.

## Prometheus monitoring options

Use the following options to export metrics to Prometheus, either by exposing them on an HTTP endpoint (pull mode) or by pushing them to a `PushGateway` (push mode).

### --monitoring.prometheus.job

The [Prometheus job name](https://github.com/prometheus/pushgateway#about-the-job-and-instance-labels) to use.

For each exported metric, DSBulk adds this value in a job label.
This job name is also used when pushing metrics to a `PushGateway`.

Default: `DSBulk`

### <a id="monitoring-prometheus-labels"></a>--monitoring.prometheus.labels

Provide a set of static labels to add to each exported metric for both pull and push mode.
Use the format `<map<string,string>>` to define the labels as a map of key-value pairs.

DSBulk automatically adds the following labels:

- `operation_id`: The current operation ID, as set by [`--engine.executionId`](engine-options.md#engine-executionId).
- `job`: The job name, as set by `--monitoring.prometheus.job`.
- `application_name`: `DataStax Bulk Loader` and the `operation_id`, such as `DataStax Bulk Loader LOAD_20240315-142530-123456`.
- `application_version`: The [DSBulk version](../02-usage.md#version).
- `driver_version`: The [Java driver version](driver-options.md) set in DSBulk's `pom.xml`.
- `client_id`: The DSBulk client UUID.

The `application_name`, `application_version`, `driver_version`, and `client_id` values are also passed from DSBulk to the Java driver, and then the driver uses those value to connect to your database.
This makes it possible to correlate data sent from the driver to your database with data sent from DSBulk to Prometheus.

Any labels set in `---monitoring.prometheus.labels` are in addition to the automatic labels.

### --monitoring.prometheus.pull.enabled (-prometheus)

> [!IMPORTANT]
> To export metrics to Prometheus, either `--monitoring.prometheus.pull.enabled` or `--monitoring.prometheus.push.enabled` must be set to `true`.
> If both options are set to `false`, no metrics can be exported directly from DSBulk to Prometheus.

Whether to expose metrics to Prometheus by scraping (pull mode):

- **`false` (default)**: Disable pull mode.
- **`true`**: Enable pull mode, allowing DSBulk and all of its metrics to be accessible over an unsecured HTTP endpoint.

  Driver metrics can be exported if they are enabled.
  For more information, see [`--monitoring.jmx`](#monitoring-jmx).

### --monitoring.prometheus.pull.hostname

If pull mode is enabled, you can provide the hostname for the metrics HTTP server to bind to.

If not set, the server binds to the wildcard address `0.0.0.0`.

Default: Not set

### --monitoring.prometheus.pull.port

If pull mode is enabled, you can specify the port number for the metrics HTTP server to bind to.

Default: `8080`

### --monitoring.prometheus.push.enabled

> [!IMPORTANT]
> To export metrics to Prometheus, either `--monitoring.prometheus.pull.enabled` or `--monitoring.prometheus.push.enabled` must be set to `true`.
> If both options are set to `false`, no metrics can be exported directly from DSBulk to Prometheus.

Whether to push metrics to a `PushGateway` (push mode):

- **`false` (default)**: Disable push mode.
- **`true`**: Enable push mode, allowing DSBulk to push metrics to a `PushGateway` URL after each `dsbulk` operation.

  Some metrics aren't exported when using push mode.
  Only some high-level metrics are exported, including the total time elapsed, the number of records processed, and the number of rows written or read.

  Notably, driver metrics aren't pushed, even if they are enabled.

### --monitoring.prometheus.push.groupBy.instance

If push mode is enabled, use this option to add an [`instance`](https://github.com/prometheus/pushgateway#about-the-job-and-instance-labels) grouping key to exported metrics:

- **`false` (default)**: Don't add the `instance` grouping key.
- **`true`**: Add the `instance` grouping key with the value set to the machine's IP address.
  This effectively groups metrics by instance, rather than by job.

### --monitoring.prometheus.push.groupBy.keys

If push mode is enabled, you can use this option to provide a set of additional fixed keys to add to the grouping keys for the exported metrics.
Use the format `<map<string,string>>` to define the grouping keys as a map of key-value pairs.

In push mode, grouping keys are also added as [labels](#monitoring-prometheus-labels) for each exported metric.

### --monitoring.prometheus.push.groupBy.operation

If push mode is enabled, use this option to add an operation ID ([`--engine.executionId`](engine-options.md#engine-executionId)) grouping key to exported metrics:

- **`false` (default)**: Don't add the `operation_id` grouping key.
- **`true`**: Add the `operation_id` grouping key with the value set by `--engine.executionId`.
  This effectively groups metrics by operation, rather than by job.

### --monitoring.prometheus.push.password

If push mode is enabled, you can set the password to authenticate with the push gateway using basic HTTP authentication.

If not set, push mode uses unauthenticated HTTP requests.

Default: Not set

### --monitoring.prometheus.push.url

If push mode is enabled, you can set the base URL for your Prometheus `PushGateway` server, such as `http://pushgateway.example.org:9091`.

Don't include the `/metrics` path.

Use [quotes and escaping](../02-usage.md#escape-and-quote-command-line-arguments) as needed.

Default: `http://localhost:9091`

### --monitoring.prometheus.push.username

If push mode is enabled, you can set the username to authenticate with the push gateway using basic HTTP authentication.

If not set, push mode uses unauthenticated HTTP requests.

Default: Not set

## See also

- [Engine options](engine-options.md)
- [Logging options](logging-options.md)

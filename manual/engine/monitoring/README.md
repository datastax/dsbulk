# Workflow Engine Monitoring

DataStax Bulk Loader uses the [Dropwizard Metrics] library to collect and report metrics.

See the [Settings page] for information about how to configure monitoring and
metrics reporting.

[Dropwizard Metrics]: http://metrics.dropwizard.io/
[Settings page]: ../../settings.md

## Available metrics

The engine collects and reports the following metrics for each operation:

<table>

<tr><th>Metric name</th><th>Metric type</th><th>Metric meaning</th></tr>

<tr><td><code>records/successful</code></td><td>meter</td><td>Number of successful records</td></tr>
<tr><td><code>records/failed</code></td><td>meter</td><td>Number of failed records</td></tr>
<tr><td><code>records/total</code></td><td>meter</td><td>Total number of records</td></tr>

<tr><td><code>mappings/successful</code></td><td>meter</td><td>Number of successful mappings</td></tr>
<tr><td><code>mappings/failed</code></td><td>meter</td><td>Number of failed mappings</td></tr>
<tr><td><code>mappings/total</code></td><td>meter</td><td>Total number of mappings</td></tr>

<tr><td><code>batches/size</code></td><td>histogram</td><td>Batch sizes</td></tr>

<tr><td><code>executor/reads/successful</code></td><td>timer</td><td>Timer for successful reads</td></tr>
<tr><td><code>executor/reads/failed</code></td><td>timer</td><td>Timer for failed reads</td></tr>
<tr><td><code>executor/reads/total</code></td><td>timer</td><td>Timer for all reads</td></tr>

<tr><td><code>executor/writes/successful</code></td><td>timer</td><td>Timer for successful writes</td></tr>
<tr><td><code>executor/writes/failed</code></td><td>timer</td><td>Timer for failed writes</td></tr>
<tr><td><code>executor/writes/total</code></td><td>timer</td><td>Timer for all writes</td></tr>

<tr><td><code>executor/reads-writes/successful</code></td><td>timer</td><td>Timer for successful reads and writes</td></tr>
<tr><td><code>executor/reads-writes/failed</code></td><td>timer</td><td>Timer for failed reads and writes</td></tr>
<tr><td><code>executor/reads-writes/total</code></td><td>timer</td><td>Timer for all reads and writes</td></tr>

<tr><td><code>executor/statements/successful</code></td><td>timer</td><td>Timer for successful statements</td></tr>
<tr><td><code>executor/statements/failed</code></td><td>timer</td><td>Timer for failed statements</td></tr>
<tr><td><code>executor/statements/total</code></td><td>timer</td><td>Timer for all statements</td></tr>

</table>

## Reporting

Metrics are collected and reported to two channels:

1. The console: a summary is printed for each component at a configurable rate (5 seconds by default). A typical output for a write operation is as follows:
    ```
    2017-08-03 10:25:50,293 INFO  Records: total: 75865, successful: 75865, failed: 0, mean: 34127 records/second
    2017-08-03 10:25:50,293 INFO  Mappings: total: 75865, successful: 75865, failed: 0, mean: 33856 mappings/second
    2017-08-03 10:25:50,294 INFO  Batches: total: 10765, avg batch size 7.05
    2017-08-03 10:25:50,296 INFO  Writes: total: 75865, successful: 72369, failed: 3496; 32865 writes/second (mean 10.34, 75p 15.98, 99p 21.99 milliseconds)
    2017-08-03 10:25:50,296 INFO  Reads: total: 0, successful: 0, failed: 0; 0 reads/second (mean 0.00, 75p 0.00, 99p 0.00 milliseconds)
    ```
2. JMX: if enabled, all the available metrics are exposed under the following MBean hierarchy:

* `com.datastax.dsbulk`
    * `<operation ID>` 
        * records
            * successful: meter
            * failed: meter
            * total: meter
        * mappings
            * successful: meter
            * failed: meter
            * total: meter
        * batches
            * size: histogram
        * executor
            * reads
                * successful: timer
                * failed: timer
                * total: timer
            * writes
                * successful: timer
                * failed: timer
                * total: timer
            * reads-writes
                * successful: timer
                * failed: timer
                * total: timer
            * statements
                * successful: timer
                * failed: timer
                * total: timer

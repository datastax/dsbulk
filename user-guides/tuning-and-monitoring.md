# Tuning and monitoring DSBulk

This guide covers how to control the rate at which DSBulk sends requests,
how to tune batching for loads, and how to watch a running operation. It is
based on the original blog series (Parts 1 and 2) and extended with options
confirmed from the DSBulk codebase.

The executor and monitoring settings apply to every operation: `load`,
`unload` and `count`. The batch settings apply to `load` only. The examples
use `load` with the `iris_with_id` table from the
[loading guide](loading.md).

## Throughput tuning

### Executor limits

Three settings cap the rate at which DSBulk sends requests:

| Option | What it limits | Default |
| --- | --- | --- |
| `--executor.maxPerSecond` | requests per second (hard limit) | −1 (unlimited) |
| `--executor.maxBytesPerSecond` | bytes per second (hard limit) | −1 (unlimited) |
| `--executor.maxInFlight` | outstanding requests at one time (soft limit) | −1 (unlimited) |

Use `maxPerSecond` to cap overall request rate:

```text
dsbulk load -url /tmp/dsbulkblog/iris.csv \
  -k dsbulkblog -t iris_with_id --executor.maxPerSecond 10
```

Use `maxInFlight` to limit concurrency without a fixed rate cap:

```text
dsbulk load -url /tmp/dsbulkblog/iris.csv \
  -k dsbulkblog -t iris_with_id --executor.maxInFlight 3
```

### Concurrent queries

`-maxConcurrentQueries` (`--engine.maxConcurrentQueries`) controls how
many queries run in parallel. The default is `AUTO` (derived from core
count). Raise or lower it to tune for your cluster.

### Batch settings

By default DSBulk groups inserts into batches by partition key
(`PARTITION_KEY` mode). The recommended tuning sequence from the settings
reference:

1. Start with `--batch.mode PARTITION_KEY` (the default).
2. If the average batch size is close to 1, raise `--batch.bufferSize`
   (default: 4 × `maxBatchStatements`).
3. If raising `bufferSize` does not help, change to `REPLICA_SET` with
   small `maxBatchStatements` or `maxSizeInBytes`.
4. Increase `maxBatchStatements` (default 32) or `maxSizeInBytes` for
   higher throughput while keeping latencies acceptable.

To disable batching entirely:

```text
dsbulk load -url /tmp/dsbulkblog/iris.csv \
  -k dsbulkblog -t iris_with_id --batch.mode DISABLED
```

### Dry run

Validate input and mapping without writing to the cluster:

```text
dsbulk load -url /tmp/dsbulkblog/iris.csv \
  -k dsbulkblog -t iris_with_id -dryRun true
```

DSBulk reads the file and checks the mapping, but sends no CQL. Review the
log directory for any mapping errors afterwards.

## Monitoring

DSBulk prints a stats line to stderr every 5 seconds (default
`--monitoring.reportRate`). Other reporters:

| Setting | Default | Description |
| --- | --- | --- |
| `--monitoring.console` | `true` | Progress line on stderr |
| `--monitoring.jmx` | `true` | JMX metrics |
| `--monitoring.csv` | `false` | CSV metrics in the log directory |
| `-prometheus` | `false` (pull) | Prometheus scrape endpoint |

To enable Prometheus scraping, set a hostname to avoid listening on all
interfaces:

```text
dsbulk load -url /tmp/dsbulkblog/iris.csv \
  -k dsbulkblog -t iris_with_id \
  --monitoring.prometheus.pull.enabled true \
  --monitoring.prometheus.pull.hostname 127.0.0.1 \
  --monitoring.prometheus.pull.port 8080
```

To push metrics to a Pushgateway:

```text
dsbulk load -url /tmp/dsbulkblog/iris.csv \
  -k dsbulkblog -t iris_with_id \
  --monitoring.prometheus.push.enabled true \
  --monitoring.prometheus.push.url http://localhost:9091
```

## Where to go next

- [loading.md](loading.md) — load data into a cluster.
- [unloading.md](unloading.md) — export data from a cluster.
- [counting.md](counting.md) — count rows and report partition statistics.
- [connection.md](connection.md) — host, port, authentication, TLS.
- [manual/settings.md](../manual/settings.md) — every option with its
  default and full description.

# DSBulk user guides

These guides describe common DSBulk operations with worked examples.
They replace a series of six blog posts that were published between 2019
and 2020. The blog posts are no longer available at their original URLs.

## Guides

| File | What it covers | Replaces |
| --- | --- | --- |
| [connection.md](connection.md) | Contact points, authentication, SSL/TLS, consistency, and configuration files | Pt. 3 — Common Settings |
| [loading.md](loading.md) | CSV and JSON load, mapping, null handling, TTL, errors, and checkpoint | Pt. 1 — Introduction and Loading; Pt. 2 — More Loading |
| [tuning-and-monitoring.md](tuning-and-monitoring.md) | Executor limits, batching, dry run, and monitoring | Pt. 2 — More Loading (tuning material) |
| [unloading.md](unloading.md) | CSV and JSON unload, column selection, compression, token splits | Pt. 4 — Unloading |
| [counting.md](counting.md) | Count modes, partitions mode, custom queries, spot-check after load | Pt. 5 — Counting |
| [cloud-and-astra.md](cloud-and-astra.md) | Astra DB, Amazon S3, GCS/Azure workarounds, cluster migration, pipe loading | Examples for Loading From Other Locations |

## Where did the blog posts go?

The six DataStax blog posts that the main `README.md` linked to are no
longer available. The guides above cover the same material, corrected and
expanded with information from the codebase.

## Known gaps

The examples in these guides come from the original blog posts and from
the DSBulk source and settings reference. They were run with DSBulk 1.11.2
against two targets:

- An Astra DB database: the loading examples, most of the unloading examples
  and the secure connect bundle option.
- A single-node Apache Cassandra 5.0 cluster: the rest of the unloading
  examples, all the counting, tuning and monitoring examples, the connection
  options `-dc`, `-cl`, `-maxRetries`, `-f`, `-c json`, `help` and
  `--version`, and the migration examples.

These examples were not run, because they need infrastructure that was not
available:

- Authentication (`-u` and `-p`) and TLS.
- Loading from and unloading to Amazon S3, Google Cloud Storage and Azure
  Blob Storage.
- The MySQL and DSEFS pipe examples.
- Behavior that is specific to DSE.

If you find an example that does not behave as described, open an issue.

## Further reading

- [../manual/settings.md](../manual/settings.md) — complete option
  reference with every shortcut, full option name, default value, and
  description.
- [../README.md](../README.md) — project overview, installation, and
  quick-start examples.

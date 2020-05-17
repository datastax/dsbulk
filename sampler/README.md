# DataStax Bulk Loader Data Size Sampler

This module contains a data size estimator and a data size sampler for statements (writes) and
rows (reads).

It is used by the [Batcher API](../batcher/api/README.md) to perform batching by data size, and by 
different workflows, to calibrate the concurrency level. 

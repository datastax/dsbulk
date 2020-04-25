# DataStax Bulk Loader Partitioning Utilities

This module contains the `TokenRangeSplitter` API, that DSBulk uses to parallelize reads by 
targeting all the token subranges simultaneously from different replicas.

This code is inspired by the equivalent API from the Spark Connector.
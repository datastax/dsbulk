# DataStax Bulk Loader Partitioning Utilities

This module contains the `PartitionGenerator` API, that DSBulk uses to parallelize reads by 
splitting the table to read into a certain number of token range scans targeting the same replicas,
which can then be fetched simultaneously from their respective replica sets.

This API is inspired by the equivalent API from the Spark Connector. DSBulk's `PartitionGenerator`
class, for instance, is adapted from Spark Connector's [`CassandraPartitionGenerator`], and other
classes in this module have counterparts in the `com.datastax.spark.connector.rdd.partitioner`
package in Spark Connector. 

[`CassandraPartitionGenerator`]: https://github.com/datastax/spark-cassandra-connector/blob/v2.4.3/spark-cassandra-connector/src/main/scala/com/datastax/spark/connector/rdd/partitioner/CassandraPartitionGenerator.scala
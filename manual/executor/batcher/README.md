# Statement Batcher

The [`StatementBatcher`] is a component that helps grouping statements together.

Grouping statements in batches before executing them is usually recommended and can improve
write throughput significantly when done right.

The batcher has two sets of methods: `batchAll` and `batchByGroupingKey`.

The `batchAll` methods are very simple: they simply group together all the given statements and
yield a single `BatchStatement`. _They should be used with caution_; if the given statements do not 
target the same replicas, the resulting batch could put a lot of pressure on the coordinator node
and lead to write throughput degradation.

The `batchByGroupingKey` methods are more elaborate: they group statements together 
in batches sharing the same grouping key.

There are two kinds of grouping key: `PARTITION_KEY` or `REPLICA_SET`; the grouping key to use 
must be specified when instantiating a batcher, and defaults to `PARTITION_KEY`. 

When `PARTITION_KEY` is used, the grouping key is the statement's routing token or
routing key, whichever is available.

When `REPLICA_SET` is used, the grouping key is the replica set owning the statement's
routing token. The `REPLICA_SET` mode might yield better results for small clusters and 
lower replication factors, but tends to perform equally well or even worse than 
`PARTITION_KEY` for larger clusters or high replication factors, due to the
increasing number of possible replica sets.
 
[`StatementBatcher`]: ../../../executor/api/src/main/java/com/datastax/oss/driver/bulk/api/statement/StatementBatcher.java

## Reactive Subclasses

`StatementBatcher` can be used as a standalone component, but it also has two useful subclasses;
with them it is possible to use `batchAll` and `batchByGroupingKey` in reactive
flows:

- [`RxJavaStatementBatcher`]: RxJava implementation.
- [`ReactorStatementBatcher`]: Reactor implementation.

[`RxJavaStatementBatcher`]: ../../executor/rxjava/src/main/java/com/datastax/loader/executor/api/batch/RxJavaStatementBatcher.java
[`ReactorStatementBatcher`]: ../../executor/reactor/src/main/java/com/datastax/loader/executor/api/batch/ReactorJavaStatementBatcher.java

### Using Statement Batcher as an RxJava operator

[`RxJavaStatementBatcher`] in turn has two subclasses that implement [`FlowableTransformer`] 
so that they can be used in a [`compose`] operation to batch statements together in an even more
simple way.

- [`RxJavaUnsortedStatementBatcher`] assumes that the upstream source delivers statements whose partition keys are
randomly distributed; when the internal buffer is full, batches are created with the accumulated
items and passed downstream.
- [`RxJavaSortedStatementBatcher`] assumes that the upstream source delivers statements whose partition keys are
already grouped together; when a new partition key is detected, a batch is created with the
accumulated items and passed downstream.
_Use this operator with caution_; if the given statements do not have their routing key already grouped together,
the resulting batch could lead to sub-optimal write performance.

[`FlowableTransformer`]: http://reactivex.io/RxJava/2.x/javadoc/io/reactivex/FlowableTransformer.html
[`compose`]: http://reactivex.io/RxJava/2.x/javadoc/io/reactivex/Flowable.html#compose(io.reactivex.FlowableTransformer)
[`RxJavaUnsortedStatementBatcher`]: ../../executor/rxjava/src/main/java/com/datastax/loader/executor/api/batch/RxJavaUnsortedStatementBatcher.java
[`RxJavaSortedStatementBatcher`]: ../../executor/rxjava/src/main/java/com/datastax/loader/executor/api/batch/RxJavaSortedStatementBatcher.java

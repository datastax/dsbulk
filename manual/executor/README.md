# DataStax Bulk Loader Executor

This library is composed of 3 modules:

1. [api](/api): The core module containing the main API.
2. [rxjava](/rxjava): Implementations of the main API using <a href="https://github.com/ReactiveX/RxJava/wiki">RxJava</a>.
2. [reactor](/reactor): Implementations of the main API using <a href="https://projectreactor.io">Reactor</a>.

The API module exports types from the following libraries:

* The [DataStax Java driver for DSE](https://github.com/datastax/java-dse-driver) 
(but it is possible to use this library with the 
[OSS driver](https://github.com/datastax/java-driver) as well).
* The [Reactive Streams API](http://www.reactive-streams.org/).
* The [Dropwizard Metrics](http://metrics.dropwizard.io/) library. 
This library is only required when using features such as metrics collection and reporting.

The API module also uses the following third-party libraries internally (but does not expose them):

* The [Guava](https://google.github.io/guava) library.

### Library contents

This library comes with several components, among which: 

1. A bulk execution unit, called `BulkExecutor`;
2. Some minor utilities, e.g. 
[`StatementBatcher`](./batcher/README.md) and 
[`TableScanner`](api/src/main/java/com/datastax/oss/driver/bulk/api/statement/TableScanner.java).
 
The first one is detailed below. For the others, refer to their respective javadocs.

## 1. BulkExecutor

The main component for performing bulk writes and reads is the [`BulkExecutor`](api/src/main/java/com/datastax/oss/driver/bulk/api/executor/BulkExecutor.java) interface.

The default implementations are:

1. RxJava: [`DefaultRxJavaBulkExecutor`](rxjava/src/main/java/com/datastax/oss/driver/bulk/api/executor/DefaultRxJavaBulkExecutor.java).
2. Reactor: [`DefaultReactorBulkExecutor`](reactor/src/main/java/com/datastax/oss/driver/bulk/api/executor/DefaultReactorBulkExecutor.java).

For example, to obtain an instance of `DefaultRxJavaBulkExecutor`, do the following:

```java
Session session = ...
BulkExecutor executor = DefaultRxJavaBulkExecutor.builder(session)
       .with... // settings, see below
       .build();
```

The project also inclusdes other implementations that leverage continuous paging when doing reads.
Such implementations can bring significant performance improvements for reads.
 
1. RxJava: [`ContinuousRxJavaBulkExecutor`](rxjava/src/main/java/com/datastax/oss/driver/bulk/api/executor/ContinuousRxJavaBulkExecutor.java).
1. Reactor: [`ContinuousReactorBulkExecutor`](rxjava/src/main/java/com/datastax/oss/driver/bulk/api/executor/ContinuousRxJavaBulkExecutor.java). 

`BulkExecutor` has many methods, but they can be grouped together according to different 
criteria for a better understanding of their behavior.

### Input types

`BulkExecutor` is basically an executor for flows of statements.

Each method accepts one of the following _input types_:

* Single statements (mostly intended for reads).
* `Iterable<? extends Statement>` (suitable for small to medium size batches).
* `Stream<? extends Statement>` (suitable for large size batches).
* `Publisher<? extends Statement>` (suitable for large size batches; `Publisher` is a [Reactive Streams type](http://www.reactive-streams.org/reactive-streams-1.0.0-javadoc/org/reactivestreams/Publisher.html)).

### Operation modes

Methods in `BulkExecutor` can also be grouped into 3 distinct _operation modes_:

* Synchronous: these methods, whose names end with `Sync`, run synchronously, i.e., 
they block until the operation is finished.
* Asynchronous: these methods, whose names end with `Async`, run asynchronously and 
return a `CompletableFuture` that will complete when the _whole operation_ completes.
* Reactive: these methods, whose names end with `Reactive`, do not execute anything 
when called but instead return a Reactive Streams `Publisher` that can be subscribed to later on.

Note: the [`Publisher` interface](http://www.reactive-streams.org/reactive-streams-1.0.0-javadoc/org/reactivestreams/Publisher.html) 
itself is not very useful. Hopefully, concrete implementations of `BulkExecutor` return concrete implementations of
`Publisher` ([`Flowable`](http://reactivex.io/RxJava/2.x/javadoc/index.html?io/reactivex/Flowable.html) for RxJava, 
[`Flux`](https://projectreactor.io/docs/core/release/api/index.html?reactor/core/publisher/Flux.html) and 
[`Mono`](https://projectreactor.io/docs/core/release/api/index.html?reactor/core/publisher/Mono.html) for Reactor), 
which makes it more convenient to consume results emitted by a `BulkExecutor`.

For example, with RxJava:

```java
RxJavaBulkExecutor executor = ...
// BulkExecutor.writeReactive() returns Publisher<WriteResult>, but RxJavaExecutor
// returns instead the covariant type Flowable<WriteResult>
Flowable<WriteResult> flowable = executor.writeReactive(...);
```

It is also possible to use the implementation-agnostic interface `BulkExecutor` and manually wrap the returned `Publisher`
into a concrete implementation. Both RxJava and Reactor offer methods to perform such a wrapping.

For example, with RxJava:

```java
BulkExecutor executor = ...
// BulkExecutor.writeReactive() returns Publisher<WriteResult>
Publisher<WriteResult> publisher = executor.writeReactive(...);
// but RxJava can convert it into Flowable<WriteResult>
Flowable<WriteResult> flowable = Flowable.fromPublisher(publisher);
```

### Operation types

Methods in `BulkExecutor` can also be grouped into 2 distinct _operation types_:

* Writes: each statement yields one single `WriteResult`;
* Reads: each statement yields 1 to N `ReadResult`s.

A `WriteResult` is a thin wrapper around the original `Statement` and the returned `ResultSet`, 
the latter exposed merely for informational purposes. A write operation does not consume the 
returned `ResultSet` as it is assumed that it is empty. 

Single-statement write operations return their `WriteResult` directly; multi-statement operations 
don’t yield results directly. In Sync and Async modes, results should be consumed by supplying an 
_optional_ `Consumer<? super WriteResult>` (otherwise, it’s a "fire and forget" operation).

Read operations, conversely, do consume all pages asynchronously; each row is emitted as a 
`ReadResult`, a thin wrapper encapsulating the original `Statement` and the driver `Row` object. 
Note that paging is handled internally and is completely abstracted away from the user.

Read operations never return results directly; in Sync and Async modes, the caller must supply a 
_mandatory_ `Consumer<? super ReadResult>`.

**Important notes about execution order**: 

To maximize throughput, `DefaultBulkExecutor` and its subclasses send and retrieve data 
asynchronously using an internal thread pool (see below for details about how to configure it).

When executing write operations, results are not guaranteed to be received in the same order 
as the order in which requests were sent. Results are emitted as soon as they are received. 

For read operations, rows pertaining to the same queryOptions are guaranteed to be retrieved in 
proper order. However, if you execute several reads in one call, results belonging to 
different queries might appear interleaved. 

If you need to enforce serialization of results, consider executing one statement at a time.

#### Examples of Write Operations

Sync Write:

```java
executor.writeSync(statements, r -> {
   System.out.println("Executed " + r.getStatement());
});
```

Async Write:

```java
CompletableFuture<Void> all = executor.writeAsync(statements, r -> {
   System.out.println("Executed " + r.getStatement());
});
all.get();
```

Reactive Write:

```java
statements // -> Publisher<Statement>
       .flatMap(executor::writeReactive) // -> Flowable<WriteResult> (RxJava) or Flux<WriteResult> (Reactor)
       .doOnNext(r -> System.out.println("Executed " + r.getStatement()))
       .blockingSubscribe();
```

#### Examples of Read Operations

Sync Read:

```java
executor.readSync("SELECT * FROM t", r -> {
   processRow(r.getRow());
});
```

Async Read:

```java
CompletableFuture<Void> all = executor.readAsync("SELECT * FROM t", r -> { 
    processRow(r.getRow()); 
});
all.get();
```

Reactive Read:

```java
executor.readReactive("SELECT * FROM ip_by_country") // -> Flowable<ReadResult> (RxJava) or Flux<ReadResult> (Reactor)
       .map(ReadResult::getRow)
       .doOnNext(this::processRow)
       .blockingSubscribe();
```

### Fault tolerance

`DefaultBulkExecutor` can be configured to operate in two _failure modes_:

* Fail-fast (default): the whole operation is stopped as soon as an error is encountered.
* Fail-safe: if a statement execution fails, the operation resumes at the next statement in the flow.

In fail-fast mode, the error is wrapped within a `BulkExecutionException` that conveys the 
failed statement. In Sync mode, the exception is thrown directly; in Async mode, the returned 
future is completed exceptionally with a `BulkExecutionException`.

There is no built-in mechanism for attempting rollbacks of already-executed statements, 
and there is no executor-level retries (but retries may occur at driver-level, depending 
on the `RetryPolicy`).

**Important**: in fail-fast mode, if a statement execution fails, all pending requests 
are abandoned; there is no guarantee that all previously submitted statements will 
complete before the executor stops.

In fail-safe mode, the error is converted into a `WriteResult` or `ReadResult` and passed 
on to consumers, along with the failed `Statement`; then the execution resumes at the 
next statement. The `Result` interface exposes two useful methods when operating in fail-safe mode:

1. `boolean isSuccess()` tells if the statement was executed successfully.
1. `Optional<BulkExecutionException> getError()` can be used to retrieve the error.

Fault tolerance is configurable per executor. To create a fail-safe executor, do the following:

```java
DefaultRxJavaBulkExecutor.builder(session)
    .failSafe()
    .build();

```

#### Examples of error handling in fail-fast mode

Sync:

```java
try {
   executor.writeSync(statements, r -> {
       System.out.println("Executed " + r.getStatement());
   });
} catch (BulkExecutionException e) {
   e.printStackTrace();
}
```

Async: 

```java
try {
   executor.writeAsync(statements, 
       r -> System.out.println("Executed " + r.getStatement()))
       .whenComplete((v, t) -> {
           if (t != null) t.printStackTrace(); // t is a BulkExecutionException
       })
       .get();
} catch (ExecutionException e) {
    e.getCause().printStackTrace(); // e.getCause() is a BulkExecutionException
}
``` 

Reactive:

```java
try {
   readCsvFileAsPublisher()                  // Publisher<...>
           .map(this::toStatement)           // Publisher<Statement>
           .flatMap(executor::writeReactive) // Flowable<WriteResult> (RxJava) or Flux<WriteResult> (Reactor)
           .doOnNext(r -> System.out.println("Executed " + r.getStatement()))
           .doOnError(t ->  t.printStackTrace())
           .blockingSubscribe();
} catch (BulkExecutionException e) {
   e.printStackTrace();
}

```

#### Examples of error handling in fail-safe mode

Sync:

```java
executor.writeSync(statements, r -> {
   if (r.getError().isPresent())
       r.getError().get().printStackTrace();
   else
       System.out.println("Executed " + r.getStatement());
});
```

Async :

```java
executor.writeAsync(statements,
       r -> {
           if (r.getError().isPresent())
               r.getError().get().printStackTrace();
           else
               System.out.println("Executed " + r.getStatement());
       })
       .get();
```

Reactive:

```java
statements
       .flatMap(executor::writeReactive)
       .doOnNext(r -> {
           if (r.getError().isPresent())
               r.getError().get().printStackTrace();
           else
               System.out.println("Executed " + r.getStatement());
       })
       .blockingSubscribe();
```

### Internal Executor

All implementations of `BulkExecutor` hold an internal `java.util.concurrent.Executor`. It can be configured as follows:

```java
BulkExecutor executor = DefaultRxJavaBulkExecutor.builder(session)
   .withExecutor(...)
   .build();
```

By default, the internal executor is a `ThreadPoolExecutor` initially empty, 
but the amount of threads is allowed to grow up to 4 times the number of available cores. 
Its `RejectedExecutionHandler` is `CallerRunsPolicy`, which is a simple way to apply backpressure 
to upstream producers.

The internal executor is responsible for sending requests, reading responses and executing 
consumers. Note that none of these operations is ever made on a driver internal thread.

**Important**: Avoid using "direct" executors such as Guava's `MoreExecutors.directExecutor()`,
i.e. executors that execute tasks on the calling thread; these will break the contract 
outlined above and force execution of consumers on a driver internal thread.

### Additional BulkExecutor Settings

Other settings the are configurable per executor include:

* Maximum number of "in-flight" requests (default 1,000): in other words, this is the maximum 
amount of concurrent uncompleted futures waiting for a response from the server. 
This acts as a safeguard against workflows that generate more requests that they can handle.
* Maximum concurrent requests per second (default 100,000): this acts as a safeguard against 
workflows that could overwhelm the cluster with more requests that it can handle.
* An optional [`ExecutionListener`](api/src/main/java/com/datastax/oss/driver/bulk/api/executor/listener/ExecutionListener.java), that allows client code to be notified of important events
processed by the executor. The following implementations are provided:
    * [`MetricsCollectingExecutionListener`](api/src/main/java/com/datastax/oss/driver/bulk/api/executor/listener/MetricsCollectingExecutionListener.java): a listener that records metrics about bulk operations;
    * [`MetricsReportingExecutionListener`](api/src/main/java/com/datastax/oss/driver/bulk/api/executor/listener/MetricsReportingExecutionListener.java): a listener that reports metrics about bulk operations;
    * [`CompositeExecutionListener`](api/src/main/java/com/datastax/oss/driver/bulk/api/executor/listener/CompositeExecutionListener.java): a listener that forwards events to all its child listeners.

Example:
    
```java
BulkExecutor executor = DefaultRxJavaBulkExecutor.builder(session)
   .withMaxInFlightRequests(...)
   .withMaxRequestsPerSecond(...)
   .withExecutionListener(...)
   .build();
```

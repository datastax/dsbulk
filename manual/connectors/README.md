# DataStax Bulk Loader Connectors

[Connectors] are responsible for connecting to an external source of data and extracting 
that data in the form of [Records], for downstream consumption by the loader workflow engine.

Connectors are created by implementing the `Connector` interface. They are then discovered through
the Service Loader API.

Available configuration settings for all connectors are documented [here](../settings.md).

## Available Connectors

The DataStax Bulk Loader ships with the following connectors:

* [CSV Connector]: a highly-configurable connector that reads field-delimited files. 
  For more information about the CSV file format, see [RFC 4180] and the [Wikipedia article on CSV format].
* [Json Connector]: a highly-configurable connector that reads Json documents.
* [CQL Connector]: a connector that reads CQL files.

[Connectors]: ../../connectors/api/src/main/java/com/datastax/dsbulk/connectors/api/Connector.java
[Records]: ../../connectors/api/src/main/java/com/datastax/dsbulk/connectors/api/Record.java
[CSV Connector]: ./csv
[Json Connector]: ./json
[CQL Connector]: ./cql
[RFC 4180]: https://tools.ietf.org/html/rfc4180
[Wikipedia article on CSV format]: https://en.wikipedia.org/wiki/Comma-separated_values

## Implementation Guidelines

### Lifecycle 

The lifecycle of a connector is as follows:

1. `configure(LoaderConfig, boolean)`
2. `init()`
3. `read()` or `write()`
4. `close()`

Connectors are allowed to be stateful. DSBulk will always preserve the order of method invocations outlined above. It will never reuse a connector for more than one operation, and will never perform both a read and a write operation on the same connector instance. 

When reading, connectors act as publishers and emit data read from their datasources as a flow of `Record`s; when writing, connectors act as a transforming function: they receive Records from an upstream publisher, write them to the external datasource, then re-emit them to downstream subscribers. 

### Read operations 

Read operations return `Suppliers` of `Publisher`s. 

All publishers are guaranteed to be subscribed only once; implementors are allowed to optimize for single-subscriber use cases, whenever possible. 

If possible, implementors are allowed to memoize suppliers whenever possible. If however the connector needs to open resources for reading (for example, open database connections), it is preferable to avoid memoizing them. 

Reading by resource: connectors that are able to distinguish natural boundaries when reading (e.g. when reading from more than one file, or reading from more than one database table) should implement `readByResource()` by emitting their records grouped by such resources; this allows DSBulk to optimize read operations, and is specially valuable if records in the original dataset are grouped by partition key inside each resource, because this natural grouping can thus be preserved. If, however, there is no distinguishable boundaries in the dataset, then this method can be derived from the `read()` method, for example (using Reactor Framework):

```java
public Supplier<? extends Publisher<Publisher<Record>>> readByResource() {
  return () -> Flux.just(read().get());
}
```  
  
DSBulk also optimizes reads according to the number of resources to read. Implementors should implement `estimatedResourceCount()` carefully. 

### Write operations 

The `write()` operation returns a `Function` that transforms `Publisher`s. 

Implementors should transform streams such that each record is written to the destination, then re-emitted to downstream subscribers, e.g. (pseudo-code using the Reactor Framework):

```java
public Function<? super Publisher<Record>, ? extends Publisher<Record>> write() {
  return upstream -> {
    return Flux.from(upstream)
      .doOnNext(writer::write)
      .doOnTerminate(writer::close);
  };
}
```  
  
Implementors are allowed to memoize functions whenever possible. If however the connector needs to open resources for writing (for example, open database connections), it is preferable to avoid memoizing them. 

### Error handling 

Unrecoverable errors (i.e., errors that put the connector in an unstable state, or in a state where no subsequent records can be read or written successfully) should be emitted as `onError` signals to downstream subscribers. DSBulk will abort the operation immediately.

Recoverable errors however (i.e., errors that do not comprise the connector's ability to emit further records successfully) should be converted to `ErrorRecord`s and emitted as such. This way, DSBulk is able to detect such errors and treat them accordingly (for example, by redirecting them to a "bad file" or by updating a counter of errors).

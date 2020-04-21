/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.connectors.api;

import com.typesafe.config.Config;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.function.Function;
import org.reactivestreams.Publisher;

/**
 * A Connector is a component capable of reading from and writing to a datasource.
 *
 * <h2>Implementation guidelines</h2>
 *
 * <p><strong>Lifecycle</strong>
 *
 * <p>The lifecycle of a connector is as follows:
 *
 * <ol>
 *   <li>{@link #configure(Config, boolean)}
 *   <li>{@link #init()}
 *   <li>{@link #read()} or {@link #write()}
 *   <li>{@link #close()}
 * </ol>
 *
 * Connectors are allowed to be stateful. DSBulk will always preserve the order of method
 * invocations outlined above. It will never reuse a connector for more than one operation, and will
 * never perform both a read and a write operation on the same connector instance.
 *
 * <p>When reading, connectors act as publishers and emit data read from their datasources as a flow
 * of {@link Record}s; when writing, connectors act as a transforming function: they receive {@link
 * Record}s from an upstream publisher, write them to the external datasource, then re-emit them to
 * downstream subscribers.
 *
 * <p><strong>Read operations</strong>
 *
 * <p>Read operations return {@link Publisher}s.
 *
 * <p>All publishers are guaranteed to be subscribed only once; implementors are allowed to optimize
 * for single-subscriber use cases.
 *
 * <p>Reading by resource: connectors that are able to distinguish natural boundaries when reading
 * (e.g. when reading from more than one file, or reading from more than one database table) should
 * implement {@link #readByResource()} by emitting their records grouped by such resources; this
 * allows DSBulk to optimize read operations, and is specially valuable if records in the original
 * dataset are grouped by partition key inside each resource, because this natural grouping can thus
 * be preserved. If, however, there is no distinguishable boundaries in the dataset, then this
 * method can be derived from the {@link #read()} method, for example (using Reactor Framework):
 *
 * <pre>
 * public Publisher&lt;? extends Publisher&lt;? extends Record&gt;&gt;&gt; readByResource() {
 *  return Flux.just(read());
 * }
 * </pre>
 *
 * DSBulk also optimizes reads according to the number of resources to read. Implementors should
 * implement {@link #estimatedResourceCount()} carefully.
 *
 * <p><strong>Write operations</strong>
 *
 * <p>The {@link #write()} operation returns a {@link Function} that transforms {@link Publisher}s.
 *
 * <p>Implementors should transform streams such that each record is written to the destination,
 * then re-emitted to downstream subscribers, e.g. (pseudo-code using the Reactor Framework):
 *
 * <pre>
 * public Function&lt;? super Publisher&lt;Record&gt;, ? extends Publisher&lt;Record&gt;&gt; write() {
 *   return upstream -&gt; {
 *     return Flux.from(upstream)
 *       .doOnNext(writer::write)
 *       .doOnTerminate(writer::close);
 *   };
 * }
 * </pre>
 *
 * <p>The transformed publisher is guaranteed to be subscribed only once; implementors are allowed
 * to optimize for single-subscriber use cases. Implementors are also allowed to memoize the
 * transforming functions.
 *
 * <p><strong>Error handling</strong>
 *
 * <p>Unrecoverable errors (i.e., errors that put the connector in an unstable state, or in a state
 * where no subsequent records can be read or written successfully) should be emitted as {@code
 * onError} signals to downstream subscribers. DSBulk will abort the operation immediately.
 *
 * <p>Recoverable errors however (i.e., errors that do not comprise the connector's ability to emit
 * further records successfully) should be converted to {@link ErrorRecord}s and emitted as such.
 * This way, DSBulk is able to detect such errors and treat them accordingly (for example, by
 * redirecting them to a "bad file" or by updating a counter of errors).
 */
public interface Connector extends AutoCloseable {

  /**
   * Reads all records from the datasource in one single flow.
   *
   * <p>This method should only be called after the connector is properly {@link #configure(Config,
   * boolean) configured} and {@link #init() initialized}.
   *
   * @return a {@link Publisher} of records read from the datasource.
   */
  @NonNull
  Publisher<Record> read();

  /**
   * Reads all records from the datasource in a flow of flows, grouped by resources.
   *
   * <p>This method might yield better performance when the number of resources to read is high
   * (that is, higher than the number of available CPU cores).
   *
   * <p>If the underlying datasource does not have the notion of resources, or cannot group records
   * by resources, then this method should behave exactly as {@link #read()} and return a publisher
   * of one single inner publisher.
   *
   * <p>This method should only be called after the connector is properly {@link #configure(Config,
   * boolean) configured} and {@link #init() initialized}.
   *
   * @return a {@link Publisher} of records read from the datasource, grouped by resources.
   */
  @NonNull
  Publisher<Publisher<Record>> readByResource();

  /**
   * Writes records to the datasource.
   *
   * <p>This method should only be called after the connector is properly {@link #configure(Config,
   * boolean) configured} and {@link #init() initialized}.
   *
   * @return A transforming {@link Function} that writes records from the upstream flow to the
   *     datasource, then emits the records written to downstream subscribers.
   */
  @NonNull
  Function<? super Publisher<Record>, ? extends Publisher<Record>> write();

  /**
   * Configures the connector.
   *
   * @param settings the connector settings.
   * @param read whether the connector should be configured for reading or writing.
   * @throws IllegalArgumentException if the connector fails to configure properly.
   */
  default void configure(@NonNull Config settings, boolean read) throws IllegalArgumentException {}

  /**
   * Initializes the connector.
   *
   * <p>This method should only be called after the connector is properly {@link #configure(Config,
   * boolean) configured}.
   *
   * @throws Exception if the connector fails to initialize properly.
   */
  default void init() throws Exception {}

  /**
   * Closes the connector.
   *
   * @throws Exception if the connector fails to close properly.
   */
  @Override
  default void close() throws Exception {}

  /**
   * Whether or not the connector supports the given feature.
   *
   * @param feature the feature to check.
   * @return {@code true} if this connector supports the feature, {@code false} otherwise.
   */
  default boolean supports(@NonNull ConnectorFeature feature) {
    return false;
  }

  /**
   * Returns metadata about the records that this connector can read or write.
   *
   * <p>This method should only be called after the connector is properly {@link #configure(Config,
   * boolean) configured} and {@link #init() initialized}.
   *
   * @return the metadata about the records that this connector can read or write.
   */
  @NonNull
  RecordMetadata getRecordMetadata();

  /**
   * Returns an estimation of the total number of resources to read.
   *
   * <p>This method should return {@code -1} if the number of resources to read is unknown, or if
   * the dataset cannot be split by resources, or if the connector cannot read, or if the connector
   * has not been configured to read but to write.
   *
   * <p>This method should only be called after the connector is properly {@link #configure(Config,
   * boolean) configured} and {@link #init() initialized}.
   *
   * @return an estimation of the total number of resources to read.
   */
  default int estimatedResourceCount() {
    return -1;
  }
}

/*
 * Copyright DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.oss.dsbulk.connectors.api;

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
 *   <li>{@link #readSingle()}, {@link #readMultiple()} or {@link #write()}
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
 * <p>All publishers returned by read methods are guaranteed to be subscribed only once;
 * implementors are allowed to optimize for single-subscriber use cases. Note however that the
 * workflow runner may invoke a read method twice during an operation, in case a data size sampling
 * is done before the operation is started.
 *
 * <p>Reading in parallel: connectors that are able to distinguish natural boundaries when reading
 * (e.g. when reading from more than one file, or reading from more than one database table) should
 * implement {@link #readMultiple()} by emitting their records grouped by such resources; this
 * allows DSBulk to optimize read operations, and is specially valuable if records in the original
 * dataset are grouped by partition key inside each resource, because this natural grouping can thus
 * be preserved. If, however, there is no distinguishable boundaries in the dataset, then this
 * method can be derived from the {@link #readSingle()} method, for example (using Reactor
 * Framework):
 *
 * <pre>
 * public Publisher&lt;Publisher&lt;Record&gt;&gt;&gt; readMultiple() {
 *  return Flux.just(read());
 * }
 * </pre>
 *
 * DSBulk will optimize reads according to the read concurrency and will call {@link
 * #readMultiple()} whenever the read concurrency is high enough. Implementors should implement
 * {@link #readConcurrency()} carefully.
 *
 * <p><strong>Write operations</strong>
 *
 * <p>The {@link #write()} method is invoked only once per operation. The write function itself,
 * however, is expected to be invoked each time the workflow runner wishes the connector to write a
 * batch of records. Therefore implementors should expect this function to be invoked many times
 * (potentially thousands of times) during an operation, and invocation may also happen in parallel
 * from different threads. Implementors should therefore make sure that their implementation is
 * thread-safe.
 *
 * <p>Implementors should transform streams such that each record is written to the destination,
 * then re-emitted to downstream subscribers, e.g. (pseudo-code using the Reactor Framework):
 *
 * <pre>
 * public Function&lt;Publisher&lt;Record&gt;,Publisher&lt;Record&gt;&gt; write() {
 *   return upstream -&gt; {
 *     return Flux.from(upstream)
 *       .doOnNext(writer::write)
 *       .doOnTerminate(writer::flush);
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
   * <p>The workflow runner will call this method rather than {@link #readMultiple()} if the
   * {@linkplain #readConcurrency() read concurrency} is too low (that is, lesser than the number of
   * available CPU cores and close to 1).
   *
   * <p>This method should only be called after the connector is properly {@link #configure(Config,
   * boolean) configured} and {@link #init() initialized}.
   *
   * @return a {@link Publisher} of records read from the datasource.
   */
  @NonNull
  Publisher<Record> readSingle();

  /**
   * Reads all records from the datasource in a flow of flows that can be consumed in parallel.
   *
   * <p>The actual number of parallel flows is expected to reflect the estimated {@linkplain
   * #readConcurrency() read concurrency}. Therefore, the workflow runner will call this method
   * rather than {@link #readSingle()} if the read concurrency is high enough (that is, equal to or
   * greater than the number of available CPU cores).
   *
   * <p>If the underlying datasource cannot split its records into multiple flows in any efficient
   * manner, then this method should behave exactly as {@link #readSingle()} and return a publisher
   * of one single inner publisher.
   *
   * <p>This method should only be called after the connector is properly {@link #configure(Config,
   * boolean) configured} and {@link #init() initialized}.
   *
   * @return a {@link Publisher} of records read from the datasource, grouped by resources.
   */
  @NonNull
  Publisher<Publisher<Record>> readMultiple();

  /**
   * Returns a function that handles writing records to the datasource.
   *
   * <p>This method should only be called after the connector is properly {@link #configure(Config,
   * boolean) configured} and {@link #init() initialized}.
   *
   * @return A transforming {@link Function} that writes records from the upstream flow to the
   *     datasource, then emits the records written to downstream subscribers.
   */
  @NonNull
  Function<Publisher<Record>, Publisher<Record>> write();

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
  default int readConcurrency() {
    return -1;
  }

  /**
   * Returns the desired write concurrency, that is, how many resources are expected be written in
   * parallel. The workflow runner is guaranteed to never invoke the {@link #write()} function by
   * more than {@code writeConcurrency} threads concurrently.
   *
   * <p>The best performance is achieved when this number is equal to or greater than the number of
   * available cores.
   *
   * <p>This method should return {@link Integer#MAX_VALUE} if the write concurrency is unbounded,
   * or at least high enough to be considered as effectively unbounded.
   *
   * <p>This method should only be called after the connector is properly {@link #configure(Config,
   * boolean) configured} and {@link #init() initialized}.
   *
   * @return an estimation of the achievable write concurrency.
   */
  int writeConcurrency();
}

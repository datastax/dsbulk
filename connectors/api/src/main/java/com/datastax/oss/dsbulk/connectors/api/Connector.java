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
 *   <li>{@link #configure(Config, boolean, boolean)}
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
 * <p>A {@link #read()} operation returns a {@link Publisher} of {@link Publisher} of {@link
 * Record}s.
 *
 * <p>All publishers returned by read methods are guaranteed to be subscribed only once;
 * implementors are allowed to optimize for single-subscriber use cases. Note however that the
 * workflow runner may invoke a read method twice during an operation, in case a data size sampling
 * is done before the operation is started. In this case, the connector is expected to "rewind" so
 * that each invocation of the read method behaves as if the data source was being read from the
 * beginning. If this is not supported, consider implementing {@link #supports(ConnectorFeature)}
 * and disallowing {@link CommonConnectorFeature#DATA_SIZE_SAMPLING}.
 *
 * <p>Reading in parallel: connectors that are able to distinguish natural boundaries when reading
 * (e.g. when reading from more than one file, or reading from more than one database table) should
 * implement {@link #read()} by emitting their records grouped by such resources; this allows DSBulk
 * to optimize read operations, and is specially valuable if records in the original dataset are
 * grouped by partition key inside each resource, because this natural grouping can thus be
 * preserved.
 *
 * <p>DSBulk will optimize reads according to the read concurrency. Implementors should implement
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
 * <p>DSBulk will optimize writes according to the write concurrency. Implementors should implement
 * {@link #writeConcurrency()} carefully.
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
   * Reads all records from the datasource in a flow of flows that can be consumed in parallel.
   *
   * <p>The inner flows are guaranteed to be consumed with a parallelism no greater than the
   * {@linkplain #readConcurrency() read concurrency}.
   *
   * <p>If the underlying datasource cannot split its records into multiple flows in any efficient
   * manner, then this method should return a publisher of one single inner publisher.
   *
   * <p>This method should only be called after the connector is properly {@link #configure(Config,
   * boolean, boolean) configured} and {@link #init() initialized}.
   *
   * @return a {@link Publisher} of records read from the datasource, grouped by resources.
   */
  @NonNull
  Publisher<Publisher<Record>> read();

  /**
   * Returns a function that handles writing records to the datasource.
   *
   * <p>This method should only be called after the connector is properly {@link #configure(Config,
   * boolean, boolean) configured} and {@link #init() initialized}.
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
   * @param retainRecordSources whether the connector should retain {@linkplain Record#getSource()
   *     sources} when emitting records; only applicable when the connector is being configured for
   *     reads.
   * @throws IllegalArgumentException if the connector fails to configure properly.
   */
  default void configure(@NonNull Config settings, boolean read, boolean retainRecordSources)
      throws IllegalArgumentException {}

  /**
   * Initializes the connector.
   *
   * <p>This method should only be called after the connector is properly {@link #configure(Config,
   * boolean, boolean) configured}.
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
   * boolean, boolean) configured} and {@link #init() initialized}.
   *
   * @return the metadata about the records that this connector can read or write.
   */
  @NonNull
  RecordMetadata getRecordMetadata();

  /**
   * Returns the desired read concurrency, that is, how many resources are expected to be read in
   * parallel.
   *
   * <p>The workflow runner is guaranteed to never consume more inner flows in parallel than {@code
   * readConcurrency}.
   *
   * <p>Depending on the read concurrency, and whether it is greater than the number of cores or
   * not, the runner will try to assign one thread to each resource, thus eliminating any context
   * switch, from the resource to read until the final statements to execute.
   *
   * <p>This method should only be called after the connector is properly {@link #configure(Config,
   * boolean, boolean) configured} and {@link #init() initialized}.
   *
   * @return the desired read concurrency; must be strictly positive, that is, greater than zero.
   */
  int readConcurrency();

  /**
   * Returns the desired write concurrency, that is, how many resources are expected to be written
   * in parallel.
   *
   * <p>The workflow runner is guaranteed to never invoke the {@link #write()} function by more than
   * {@code writeConcurrency} threads concurrently.
   *
   * <p>Depending on the write concurrency, and whether it is greater than the number of cores or
   * not, the runner will decide to assign one thread to each resource, thus eliminating any context
   * switch, from the decoded rows until the final resource being written.
   *
   * <p>This method should only be called after the connector is properly {@link #configure(Config,
   * boolean, boolean) configured} and {@link #init() initialized}.
   *
   * @return the desired write concurrency; must be strictly positive, that is, greater than zero.
   */
  int writeConcurrency();
}

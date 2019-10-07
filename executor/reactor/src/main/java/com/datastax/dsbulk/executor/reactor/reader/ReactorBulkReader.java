/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.executor.reactor.reader;

import com.datastax.dsbulk.executor.api.exception.BulkExecutionException;
import com.datastax.dsbulk.executor.api.reader.ReactiveBulkReader;
import com.datastax.dsbulk.executor.api.result.ReadResult;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.cql.Statement;
import java.util.stream.Stream;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;

/**
 * A reactive execution unit for bulk read operations using <a
 * href="https://projectreactor.io">Reactor</a>.
 *
 * <p>Methods of this interface all return a {@link Flux} of read results.
 */
public interface ReactorBulkReader extends ReactiveBulkReader {

  /**
   * Executes the given read statement reactively.
   *
   * @param statement The statement to execute.
   * @return A {@link Flux Flux} that will emit one single read result.
   * @throws BulkExecutionException if the operation cannot complete normally.
   */
  @Override
  default Flux<ReadResult> readReactive(String statement) throws BulkExecutionException {
    return readReactive(SimpleStatement.newInstance(statement));
  }

  /**
   * Executes the given read statement reactively.
   *
   * @param statement The statement to execute.
   * @return A {@link Flux Flux} that will emit one single read result.
   * @throws BulkExecutionException if the operation cannot complete normally.
   */
  @Override
  Flux<ReadResult> readReactive(Statement<?> statement) throws BulkExecutionException;

  /**
   * Executes the given stream of read statements reactively.
   *
   * @param statements The statements to execute.
   * @return A {@link Flux Flux} of read results.
   * @throws BulkExecutionException if the operation cannot complete normally.
   */
  @Override
  Flux<ReadResult> readReactive(Stream<? extends Statement> statements)
      throws BulkExecutionException;

  /**
   * Executes the given iterable of read statements reactively.
   *
   * @param statements The statements to execute.
   * @return A {@link Flux Flux} of read results.
   * @throws BulkExecutionException if the operation cannot complete normally.
   */
  @Override
  Flux<ReadResult> readReactive(Iterable<? extends Statement> statements)
      throws BulkExecutionException;

  /**
   * Executes the given Flux of read statements reactively.
   *
   * @param statements The statements to execute.
   * @return A {@link Flux Flux} of read results.
   * @throws BulkExecutionException if the operation cannot complete normally.
   */
  @Override
  Flux<ReadResult> readReactive(Publisher<? extends Statement> statements)
      throws BulkExecutionException;
}

/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.executor.rxjava.reader;

import com.datastax.dsbulk.executor.api.exception.BulkExecutionException;
import com.datastax.dsbulk.executor.api.reader.ReactiveBulkReader;
import com.datastax.dsbulk.executor.api.result.ReadResult;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.cql.Statement;
import io.reactivex.Flowable;
import java.util.stream.Stream;
import org.reactivestreams.Publisher;

/**
 * A reactive execution unit for bulk read operations using <a
 * href="https://github.com/ReactiveX/RxJava/wiki">RxJava</a>.
 *
 * <p>Methods of this interface all return a {@link Flowable} of read results.
 */
public interface RxJavaBulkReader extends ReactiveBulkReader {

  /**
   * Executes the given read statement reactively.
   *
   * @param statement The statement to execute.
   * @return A {@link Flowable flowable} that will emit one single read result.
   * @throws BulkExecutionException if the operation cannot complete normally.
   */
  @Override
  default Flowable<ReadResult> readReactive(String statement) throws BulkExecutionException {
    return readReactive(SimpleStatement.newInstance(statement));
  }

  /**
   * Executes the given read statement reactively.
   *
   * @param statement The statement to execute.
   * @return A {@link Flowable flowable} that will emit one single read result.
   * @throws BulkExecutionException if the operation cannot complete normally.
   */
  @Override
  Flowable<ReadResult> readReactive(Statement<?> statement) throws BulkExecutionException;

  /**
   * Executes the given stream of read statements reactively.
   *
   * @param statements The statements to execute.
   * @return A {@link Flowable flowable} of read results.
   * @throws BulkExecutionException if the operation cannot complete normally.
   */
  @Override
  Flowable<ReadResult> readReactive(Stream<? extends Statement> statements)
      throws BulkExecutionException;

  /**
   * Executes the given iterable of read statements reactively.
   *
   * @param statements The statements to execute.
   * @return A {@link Flowable flowable} of read results.
   * @throws BulkExecutionException if the operation cannot complete normally.
   */
  @Override
  Flowable<ReadResult> readReactive(Iterable<? extends Statement> statements)
      throws BulkExecutionException;

  /**
   * Executes the given flowable of read statements reactively.
   *
   * @param statements The statements to execute.
   * @return A {@link Flowable flowable} of read results.
   * @throws BulkExecutionException if the operation cannot complete normally.
   */
  @Override
  Flowable<ReadResult> readReactive(Publisher<? extends Statement> statements)
      throws BulkExecutionException;
}

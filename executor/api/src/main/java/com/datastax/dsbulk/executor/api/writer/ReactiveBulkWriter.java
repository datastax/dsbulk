/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.executor.api.writer;

import com.datastax.dsbulk.executor.api.exception.BulkExecutionException;
import com.datastax.dsbulk.executor.api.result.WriteResult;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.cql.Statement;
import java.util.stream.Stream;
import org.reactivestreams.Publisher;

/**
 * A reactive execution unit for bulk write operations.
 *
 * <p>Methods of this interface all return a {@link Publisher} of write results.
 */
public interface ReactiveBulkWriter extends AutoCloseable {

  /**
   * Executes the given write statement reactively. The returned publisher is guaranteed to only
   * ever emit one item, or throw an error if the execution failed.
   *
   * @param statement The statement to execute.
   * @return A {@link Publisher publisher} that will emit one single write result.
   * @throws BulkExecutionException if the operation cannot complete normally.
   */
  default Publisher<WriteResult> writeReactive(String statement) throws BulkExecutionException {
    return writeReactive(SimpleStatement.newInstance(statement));
  }

  /**
   * Executes the given write statement reactively. The returned publisher is guaranteed to only
   * ever emit one item, or throw an error if the execution failed.
   *
   * @param statement The statement to execute.
   * @return A {@link Publisher publisher} that will emit one single write result.
   * @throws BulkExecutionException if the operation cannot complete normally.
   */
  Publisher<WriteResult> writeReactive(Statement statement) throws BulkExecutionException;

  /**
   * Executes the given stream of write statements reactively.
   *
   * @param statements The statements to execute.
   * @return A {@link Publisher publisher} of write results.
   * @throws BulkExecutionException if the operation cannot complete normally.
   */
  Publisher<WriteResult> writeReactive(Stream<? extends Statement> statements)
      throws BulkExecutionException;

  /**
   * Executes the given iterable of write statements reactively.
   *
   * @param statements The statements to execute.
   * @return A {@link Publisher publisher} of write results.
   * @throws BulkExecutionException if the operation cannot complete normally.
   */
  Publisher<WriteResult> writeReactive(Iterable<? extends Statement> statements)
      throws BulkExecutionException;

  /**
   * Executes the given publisher of write statements reactively.
   *
   * @param statements The statements to execute.
   * @return A {@link Publisher publisher} of write results.
   * @throws BulkExecutionException if the operation cannot complete normally.
   */
  Publisher<WriteResult> writeReactive(Publisher<? extends Statement> statements)
      throws BulkExecutionException;
}

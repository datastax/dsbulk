/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.executor.api.reader;

import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.Statement;
import com.datastax.dsbulk.executor.api.exception.BulkExecutionException;
import com.datastax.dsbulk.executor.api.result.ReadResult;
import java.util.stream.Stream;
import org.reactivestreams.Publisher;

/**
 * A reactive execution unit for bulk read operations.
 *
 * <p>Methods of this interface all return a {@link Publisher} of read results.
 */
public interface ReactiveBulkReader extends AutoCloseable {

  /**
   * Executes the given read statement reactively.
   *
   * @param statement The statement to execute.
   * @return A {@link Publisher publisher} that will emit one single read result.
   * @throws BulkExecutionException if the operation cannot complete normally.
   */
  default Publisher<ReadResult> readReactive(String statement) throws BulkExecutionException {
    return readReactive(new SimpleStatement(statement));
  }

  /**
   * Executes the given read statement reactively.
   *
   * @param statement The statement to execute.
   * @return A {@link Publisher publisher} that will emit one single read result.
   * @throws BulkExecutionException if the operation cannot complete normally.
   */
  Publisher<ReadResult> readReactive(Statement statement) throws BulkExecutionException;

  /**
   * Executes the given stream of read statements reactively.
   *
   * @param statements The statements to execute.
   * @return A {@link Publisher publisher} of read results.
   * @throws BulkExecutionException if the operation cannot complete normally.
   */
  Publisher<ReadResult> readReactive(Stream<? extends Statement> statements)
      throws BulkExecutionException;

  /**
   * Executes the given iterable of read statements reactively.
   *
   * @param statements The statements to execute.
   * @return A {@link Publisher publisher} of read results.
   * @throws BulkExecutionException if the operation cannot complete normally.
   */
  Publisher<ReadResult> readReactive(Iterable<? extends Statement> statements)
      throws BulkExecutionException;

  /**
   * Executes the given publisher of read statements reactively.
   *
   * @param statements The statements to execute.
   * @return A {@link Publisher publisher} of read results.
   * @throws BulkExecutionException if the operation cannot complete normally.
   */
  Publisher<ReadResult> readReactive(Publisher<? extends Statement> statements)
      throws BulkExecutionException;
}

/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.executor.rxjava.reader;

import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.Statement;
import com.datastax.dsbulk.executor.api.exception.BulkExecutionException;
import com.datastax.dsbulk.executor.api.reader.ReactiveBulkReader;
import com.datastax.dsbulk.executor.api.result.ReadResult;
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
  default Flowable<ReadResult> readReactive(String statement) throws BulkExecutionException {
    return readReactive(new SimpleStatement(statement));
  }

  /**
   * Executes the given read statement reactively.
   *
   * @param statement The statement to execute.
   * @return A {@link Flowable flowable} that will emit one single read result.
   * @throws BulkExecutionException if the operation cannot complete normally.
   */
  Flowable<ReadResult> readReactive(Statement statement) throws BulkExecutionException;

  /**
   * Executes the given stream of read statements reactively.
   *
   * @param statements The statements to execute.
   * @return A {@link Flowable flowable} of read results.
   * @throws BulkExecutionException if the operation cannot complete normally.
   */
  Flowable<ReadResult> readReactive(Stream<? extends Statement> statements)
      throws BulkExecutionException;

  /**
   * Executes the given iterable of read statements reactively.
   *
   * @param statements The statements to execute.
   * @return A {@link Flowable flowable} of read results.
   * @throws BulkExecutionException if the operation cannot complete normally.
   */
  Flowable<ReadResult> readReactive(Iterable<? extends Statement> statements)
      throws BulkExecutionException;

  /**
   * Executes the given flowable of read statements reactively.
   *
   * @param statements The statements to execute.
   * @return A {@link Flowable flowable} of read results.
   * @throws BulkExecutionException if the operation cannot complete normally.
   */
  Flowable<ReadResult> readReactive(Publisher<? extends Statement> statements)
      throws BulkExecutionException;
}

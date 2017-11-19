/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.executor.rxjava.writer;

import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.Statement;
import com.datastax.dsbulk.executor.api.exception.BulkExecutionException;
import com.datastax.dsbulk.executor.api.result.WriteResult;
import com.datastax.dsbulk.executor.api.writer.ReactiveBulkWriter;
import io.reactivex.Flowable;
import java.util.stream.Stream;
import org.reactivestreams.Publisher;

/**
 * A reactive execution unit for bulk write operations using <a
 * href="https://github.com/ReactiveX/RxJava/wiki">RxJava</a>.
 *
 * <p>Methods of this interface all return a {@link Flowable} of write results.
 */
public interface RxJavaBulkWriter extends ReactiveBulkWriter {

  /**
   * Executes the given write statement reactively. The returned flowable is guaranteed to only ever
   * emit one item, or throw an error if the execution failed.
   *
   * @param statement The statement to execute.
   * @return A {@link Flowable flowable} that will emit one single write result.
   * @throws BulkExecutionException if the operation cannot complete normally.
   */
  @Override
  default Flowable<WriteResult> writeReactive(String statement) throws BulkExecutionException {
    return writeReactive(new SimpleStatement(statement));
  }

  /**
   * Executes the given write statement reactively. The returned flowable is guaranteed to only ever
   * emit one item, or throw an error if the execution failed.
   *
   * @param statement The statement to execute.
   * @return A {@link Flowable flowable} that will emit one single write result.
   * @throws BulkExecutionException if the operation cannot complete normally.
   */
  @Override
  Flowable<WriteResult> writeReactive(Statement statement) throws BulkExecutionException;

  /**
   * Executes the given stream of write statements reactively.
   *
   * @param statements The statements to execute.
   * @return A {@link Flowable flowable} of write results.
   * @throws BulkExecutionException if the operation cannot complete normally.
   */
  @Override
  Flowable<WriteResult> writeReactive(Stream<? extends Statement> statements)
      throws BulkExecutionException;

  /**
   * Executes the given iterable of write statements reactively.
   *
   * @param statements The statements to execute.
   * @return A {@link Flowable flowable} of write results.
   * @throws BulkExecutionException if the operation cannot complete normally.
   */
  @Override
  Flowable<WriteResult> writeReactive(Iterable<? extends Statement> statements)
      throws BulkExecutionException;

  /**
   * Executes the given flowable of write statements reactively.
   *
   * @param statements The statements to execute.
   * @return A {@link Flowable flowable} of write results.
   * @throws BulkExecutionException if the operation cannot complete normally.
   */
  @Override
  Flowable<WriteResult> writeReactive(Publisher<? extends Statement> statements)
      throws BulkExecutionException;
}

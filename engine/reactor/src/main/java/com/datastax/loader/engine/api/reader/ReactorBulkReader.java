/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.loader.engine.api.reader;

import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.Statement;
import com.datastax.loader.engine.api.exception.BulkExecutionException;
import com.datastax.loader.engine.api.result.ReadResult;
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
  default Flux<ReadResult> readReactive(String statement) throws BulkExecutionException {
    return readReactive(new SimpleStatement(statement));
  }

  /**
   * Executes the given read statement reactively.
   *
   * @param statement The statement to execute.
   * @return A {@link Flux Flux} that will emit one single read result.
   * @throws BulkExecutionException if the operation cannot complete normally.
   */
  Flux<ReadResult> readReactive(Statement statement) throws BulkExecutionException;

  /**
   * Executes the given stream of read statements reactively.
   *
   * @param statements The statements to execute.
   * @return A {@link Flux Flux} of read results.
   * @throws BulkExecutionException if the operation cannot complete normally.
   */
  Flux<ReadResult> readReactive(Stream<? extends Statement> statements)
      throws BulkExecutionException;

  /**
   * Executes the given iterable of read statements reactively.
   *
   * @param statements The statements to execute.
   * @return A {@link Flux Flux} of read results.
   * @throws BulkExecutionException if the operation cannot complete normally.
   */
  Flux<ReadResult> readReactive(Iterable<? extends Statement> statements)
      throws BulkExecutionException;

  /**
   * Executes the given Flux of read statements reactively.
   *
   * @param statements The statements to execute.
   * @return A {@link Flux Flux} of read results.
   * @throws BulkExecutionException if the operation cannot complete normally.
   */
  Flux<ReadResult> readReactive(Publisher<? extends Statement> statements)
      throws BulkExecutionException;
}

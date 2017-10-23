/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.executor.api.result;

import com.datastax.driver.core.Statement;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;

/**
 * A factory for {@link Queue} instances to use when executing read requests to store {@link
 * ReadResult}s.
 */
@FunctionalInterface
public interface QueueFactory {

  /**
   * The default {@link QueueFactory}. It will create {@link ArrayBlockingQueue} instances whose
   * size is 4 times the statement's {@link Statement#getFetchSize() fetch size}.
   */
  QueueFactory DEFAULT = statement -> new ArrayBlockingQueue<>(statement.getFetchSize() * 4);

  /**
   * Creates a new queue for the execution of the given (read) statement.
   *
   * @param statement The (read) statement to execute.
   * @return a new {@link Queue}.
   */
  Queue<ReadResult> newQueue(Statement statement);
}

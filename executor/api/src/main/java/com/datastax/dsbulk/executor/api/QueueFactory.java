/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.executor.api;

import com.datastax.driver.core.Statement;
import java.util.Queue;

@FunctionalInterface
public interface QueueFactory<T> {

  Queue<T> newQueue(Statement statement);
}

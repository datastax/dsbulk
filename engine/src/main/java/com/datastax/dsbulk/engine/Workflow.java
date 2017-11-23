/*
 * Copyright DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.engine;

public interface Workflow extends AutoCloseable {

  void init() throws Exception;

  void execute() throws Exception;

  @Override
  void close() throws Exception;
}

/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.engine;

public class WorkflowEngineExecutionException extends Exception {

  public WorkflowEngineExecutionException(String message, Throwable cause) {
    super(message, cause);
  }
}
